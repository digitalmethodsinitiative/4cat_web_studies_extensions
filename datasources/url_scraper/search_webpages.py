"""
Selenium Webpage HTML Scraper

Currently designed around Firefox, but can also work with Chrome; results may vary
"""
from urllib.parse import urlparse
import datetime
import random
from ural import is_url
from requests.utils import requote_uri

from common.config_manager import config
from extensions.web_studies.selenium_scraper import SeleniumSearch
from common.lib.exceptions import QueryParametersException, ProcessorInterruptedException, QueryNeedsExplicitConfirmationException
from common.lib.item_mapping import MappedItem
from common.lib.user_input import UserInput
from common.lib.helpers import url_to_hash

class SearchWithSelenium(SeleniumSearch):
    """
    Get HTML via the Selenium webdriver and Firefox browser
    """
    type = "url_scraper-search"  # job ID
    category = "Search"  # category
    title = "Selenium Url Collector"  # title displayed in UI
    description = "Query a list of urls to scrape HTML source code"  # description displayed in UI
    extension = "ndjson"

    @classmethod
    def get_options(cls, parent_dataset=None, user=None):
        options = {
            "intro-1": {
                "type": UserInput.OPTION_INFO,
                "help": "Collect text and HTML from a provided list of URLs/links using a Firefox browser."
                        "This uses [Selenium](https://selenium-python.readthedocs.io/) in combination with "
                        "a [Firefox webdriver](https://github.com/mozilla/geckodriver/releases)."
                        "\n"
                        "Using a browser more closely mimics a person compared with simple HTML requests. It "
                        "will also render JavaScript that starts as soon as a url is retrieved by a browser. "
            },
            "query-info": {
                "type": UserInput.OPTION_INFO,
                "help": "Please enter a list of urls one per line. Include the protocol (e.g., http://, https://)."
            },
            "query": {
                "type": UserInput.OPTION_TEXT_LARGE,
                "help": "List of urls"
            },

        }
        if config.get("selenium.display_advanced_options", False, user=user):
            options["subpages"] = {
                "type": UserInput.OPTION_TEXT,
                "help": "Crawl additional links/subpages",
                "min": 0,
                "max": 5,
                "default": 0,
                "tooltip": "If enabled, the scraper will also crawl and collect random links found on the provided page."
            }

        return options

    def get_items(self, query):
        """
        Separate and check urls, then loop through each and collects the HTML.

        :param query:
        :return:
        """
        scrape_additional_subpages = self.parameters.get("subpages", 0)
        urls_to_scrape = [{'url':url, 'base_url':url, 'num_additional_subpages': scrape_additional_subpages, 'subpage_links':[]} for url in query.get('urls')]

        # Do not scrape the same site twice
        scraped_urls = set()
        num_urls = len(urls_to_scrape)
        if scrape_additional_subpages:
            num_urls = num_urls * (scrape_additional_subpages + 1)

        done = 0
        count = 0
        unable_to_scrape = []
        while urls_to_scrape:
            count += 1
            if self.interrupted:
                raise ProcessorInterruptedException("Interrupted while scraping urls from the Web Archive")

            self.dataset.update_progress(done / num_urls)
            self.dataset.update_status("Captured %i of %i possible URLs" % (done, num_urls))

            # Grab first url
            url_obj = urls_to_scrape.pop(0)
            url = url_obj['url']
            num_additional_subpages = url_obj['num_additional_subpages']
            result = {
                "id": url_to_hash(url),
                "base_url": url_obj['base_url'],
                "url": url,
                "final_url": None,
                "subject": None,
                "body": None,
                "html": None,
                "detected_404": None,
                "timestamp": None,
                "error": '',
                "embedded_iframes": [],
            }

            attempts = 0
            success = False
            scraped_page = {}
            while attempts < 2:
                attempts += 1
                try:
                    scraped_page = self.simple_scrape_page(url, extract_links=True)
                except Exception as e:
                    self.dataset.log('Url %s unable to be scraped with error: %s' % (url, str(e)))
                    self.restart_selenium()
                    result['error'] = 'SCRAPE ERROR:\n' + str(e) + '\n'
                    continue

                # Check for results and collect text
                if scraped_page:
                    scraped_page['text'] = self.scrape_beautiful_text(scraped_page['page_source'])
                else:
                    # Hard Fail?
                    self.dataset.log('Hard fail; no page source on url: %s' % url)
                    continue

                # Check for 404 errors
                if scraped_page['detected_404']:
                    four_oh_four_error = '404 detected on url: %s' % url
                    scraped_page['error'] = four_oh_four_error if not scraped_page.get('error', False) else scraped_page['error'] + four_oh_four_error
                    break
                else:
                    success = True
                    scraped_urls.add(url)
                    break

            if success:
                self.dataset.log('Collected: %s' % url)
                done += 1
                # Update result and yield it
                result['final_url'] = scraped_page.get('final_url')
                result['body'] = scraped_page.get('text')
                result['subject'] = scraped_page.get('page_title')
                result['html'] = scraped_page.get('page_source')
                result['detected_404'] = scraped_page.get('detected_404')
                result['timestamp'] = int(datetime.datetime.now().timestamp())
                result['error'] = scraped_page.get('error') # This should be None...
                result['selenium_links'] = scraped_page.get('links', [])

                # Collect links from page source
                domain = urlparse(url).scheme + '://' + urlparse(url).netloc
                num_of_links, links = self.get_beautiful_links(scraped_page['page_source'], domain)
                result['scraped_links'] = links

                # Scrape iframes as well
                # These could be visible on the page, but are not in the page source
                # TODO: Selenium can select iframes and pull the source that way; this may be better than the below method
                iframe_links = self.get_beautiful_iframe_links(scraped_page['page_source'])
                while iframe_links:
                    link = iframe_links.pop(0)
                    if not link:
                        # Unable to extract link to iframe source
                        continue
                    if not is_url(link):
                        self.dataset.log(f"Skipping iframe page source found on {scraped_page.get('final_url')} due to malformed URL: {link}")
                        continue

                    result['embedded_iframes'].append(link)
                    try:
                        iframe_page = self.simple_scrape_page(link, extract_links=True)
                    except Exception as e:
                        self.dataset.log(f"Unable to collect iframe page source found on {scraped_page.get('final_url')}: {link} with error: {str(e)}")
                        continue

                    if iframe_page:
                        result['body'] += ['\n'] + self.scrape_beautiful_text(iframe_page['page_source'])
                        result['html'] += '\n' + iframe_page['page_source']
                        result['selenium_links'] += iframe_page.get('links', [])
                        # Collect links from page source
                        domain = urlparse(link).scheme + '://' + urlparse(link).netloc
                        num_of_links, links = self.get_beautiful_links(iframe_page['page_source'], domain)
                        result['scraped_links'] += links

                # Check if additional subpages need to be crawled
                if num_additional_subpages > 0:
                    # Check if any link from base_url are available
                    if not url_obj['subpage_links']:
                        # If not, use this pages links collected above
                        # TODO could also use selenium detected links; results vary, check as they are also being stored
                        # Randomize links (else we end up with mostly menu items at the top of webpages)
                        random.shuffle(links)
                    else:
                        links = url_obj['subpage_links']

                    # Find the first link that has not been previously scraped
                    while links:
                        link = links.pop(0)
                        if self.check_exclude_link(link.get('url'), scraped_urls, base_url='.'.join(urlparse(url_obj['base_url']).netloc.split('.')[1:])):
                            # Add it to be scraped next
                            urls_to_scrape.insert(0, {
                                'url': link.get('url'),
                                'base_url': url_obj['base_url'],
                                'num_additional_subpages': num_additional_subpages - 1, # Make sure to request less additional pages
                                'subpage_links':links,
                            })
                            break

                yield result

            else:
                # Page was not successfully scraped
                # Still need subpages?
                if num_additional_subpages > 0:
                    # Add the next one if it exists
                    links = url_obj['subpage_links']
                    while links:
                        link = links.pop(0)
                        if self.check_exclude_link(link.get('url'), scraped_urls, base_url='.'.join(urlparse(url_obj['base_url']).netloc.split('.')[1:])):
                            # Add it to be scraped next
                            urls_to_scrape.insert(0, {
                                'url': link.get('url'),
                                'base_url': url_obj['base_url'],
                                'num_additional_subpages': num_additional_subpages - 1, # Make sure to request less additional pages
                                'subpage_links':links,
                            })
                            break
                # Unsure if we should return ALL failures, but certainly the originally supplied urls
                result['timestamp'] = int(datetime.datetime.now().timestamp())
                if scraped_page:
                    result['error'] = scraped_page.get('error')
                else:
                    # missing error...
                    result['error'] = 'Unable to scrape'
                unable_to_scrape.append(f"Unable to scrape url {url}: {result['error']}")

                yield result

        if unable_to_scrape:
            self.dataset.log("Unable to scrape the following urls:")
            for error in unable_to_scrape:
                self.dataset.log(error)
        self.dataset.update_status(f"Collected {done} of {num_urls} possible URLs." + (f"; Unable to scrape {len(unable_to_scrape)} urls. See log for details." if unable_to_scrape else ''), is_final=True)

    @staticmethod
    def map_item(page_result):
        """
        Map webpage result from JSON to 4CAT expected values.

        This makes some minor changes to ensure processors can handle specific
        columns and "export to csv" has formatted data.

        :param json page_result:  Object with original datatypes
        :return dict:  Dictionary in the format expected by 4CAT
        """
        if not page_result.get("id"):
            page_result["id"] = url_to_hash(page_result.get("url"))
        # Convert list of text strings to one string
        page_result['body'] = '\n'.join(page_result.get('body')) if page_result.get('body') else ''
        # Convert list of link objects to comma seperated urls
        page_result['scraped_links'] = ','.join([link.get('url') for link in page_result.get('scraped_links')]) if page_result.get('scraped_links') else ''
        # Convert list of links to comma seperated urls
        page_result['selenium_links'] = ','.join(map(str,page_result.get('selenium_links'))) if type(page_result.get('selenium_links')) == list else page_result.get('selenium_links', '')

        return MappedItem(page_result)


    @staticmethod
    def validate_query(query, request, user):
        """
        Validate input for a dataset query on the Selenium Webpage Scraper.

        Will raise a QueryParametersException if invalid parameters are
        encountered. Parameters are additionally sanitised.

        :param dict query:  Query parameters, from client-side.
        :param request:  Flask request
        :param User user:  User object of user who has submitted the query
        :return dict:  Safe query parameters
        """
        if not query.get("query", None):
            raise QueryParametersException("Please provide a List of urls.")

        validated_urls, invalid_urls = SeleniumSearch.validate_urls_from_params(query.get("query", ""))

        if invalid_urls:
            if not query.get("frontend-confirm"):
                raise QueryNeedsExplicitConfirmationException(f"Invalid Urls detected: \n{invalid_urls} \nContinue anyway?")
            else:
                validated_urls += invalid_urls

        if not validated_urls:
            raise QueryParametersException("No Urls detected!")

        return {
            "urls": validated_urls,
            "subpages": query.get("subpages", 0)
            }
