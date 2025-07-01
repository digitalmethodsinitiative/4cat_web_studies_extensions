"""
Selenium Webpage HTML Scraper

Currently designed around Firefox, but can also work with Chrome; results may vary
"""
import traceback
import datetime
import time
from urllib.parse import unquote, urlparse, parse_qs
from ural import is_url

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import StaleElementReferenceException, InvalidSessionIdException, TimeoutException

from common.config_manager import config
from extensions.web_studies.selenium_scraper import SeleniumSearch
from common.lib.exceptions import QueryParametersException, ProcessorInterruptedException
from common.lib.item_mapping import MappedItem
from common.lib.user_input import UserInput
from common.lib.helpers import url_to_hash

class AmazonProductSearch(SeleniumSearch):
    """
    Get HTML via the Selenium webdriver and Firefox browser
    """
    type = "amazon_products-search"  # job ID
    category = "Search"  # category
    title = "Amazon Related Products"  # title displayed in UI
    description = "Collect related products from a list of Amazon product links"  # description displayed in UI
    extension = "ndjson"
    eager_selenium = True

    # Known carousels to collect recommendations
    config = {
        "cache.amazon.carousels": {
            "type": UserInput.OPTION_TEXT_JSON,
            "help": "Amazon Carousels",
            "tooltip": "Automatically updated when new carousels are detected",
            "default": [
                "Customers who bought this item also bought",
                "Customers who viewed this also viewed",
                "Products related to this item",
                "Customers who viewed items in your browsing history also viewed",
                "What do customers buy after viewing this item?",
                "Similar items that ship from close to you",
                "All Recommendations", # SPECIAL type used to crawl all recommendations
             ],
            "indirect": True
        },
    }

    @classmethod
    def get_options(cls, parent_dataset=None, user=None):
        options = {
            "intro-1": {
                "type": UserInput.OPTION_INFO,
                "help": "Collect related products from Amazon by providing a list of Amazon product URLs. This will "
                        "collect product details as well as recommended related products such as 'Customers who bouth "
                        "this item also bought' and 'What do customers buy after viewing this item?'."
            },
            "query-info": {
                "type": UserInput.OPTION_INFO,
                "help": "Please enter a list of Amazon product urls one per line."
            },
            "query": {
                "type": UserInput.OPTION_TEXT_LARGE,
                "help": "List of urls"
            },
            "depth": {
                "type": UserInput.OPTION_TEXT,
                "help": "Recommendation depth.",
                "min": 0,
                "max": 3,
                "default": 0,
                "tooltip": "0 only collects products from provided links; otherwise collect additional products from recommended links selected below."
            },
            "rec_type": {
                "type": UserInput.OPTION_MULTI_SELECT,
                "help": "Recommended products to collect.",
                "options": {k:k for k in config.get("cache.amazon.carousels", [], user=user)},
                "default": [],
                "tooltip": "Select the types of recommended products to additionally collect. If none are selected, only the provided urls will be collected. If \"All Recommendations\" is selected, all recommended products will be collected regardless of other selections."
            },
        }
        if config.get('selenium.firefox_extensions', user=user) and config.get('selenium.firefox_extensions', user=user).get('i_dont_care_about_cookies', {}).get('path'):
            options["ignore-cookies"] = {
               "type": UserInput.OPTION_TOGGLE,
               "help": "Attempt to ignore cookie walls",
               "default": False,
               "tooltip": 'If enabled, a firefox extension will attempt to "agree" to any cookie walls automatically.'
            }

        return options

    @classmethod
    def is_compatible_with(cls, module=None, user=None):
        """
        Allow processor on image sets

        :param module: Module to determine compatibility with
        """
        return config.get('selenium.installed', False, user=user)

    def get_items(self, query):
        """
        Separate and check urls, then loop through each and collects the HTML.

        :param query:
        :return:
        """
        self.dataset.log('Query: %s' % str(query))
        depth = query.get('depth', 0)
        subpage_types = query.get('rec_type', [])
        urls_to_collect = [{"url": AmazonProductSearch.normalize_amazon_links(url), 'current_depth': 0, "retries":0} for url in query.get('urls')]

        # Load known carousels (load into memory to avoid repeated database calls)
        known_carousels = self.config.get("cache.amazon.carousels", [])
        added_carousels = set() # If we have already added a carousel, do not add it again

        # Do not scrape the same page twice
        collected_urls = set()
        num_urls = len(urls_to_collect)
        urls_collected = 0
        missing_carousels = 0
        potential_captcha = 0
        consecutive_captchas = 0
        count = 0

        while urls_to_collect:
            count += 1
            if self.interrupted:
                raise ProcessorInterruptedException("Interrupted while collecting Amazon product URLs")

            # Get the next URL to collect
            url_obj = urls_to_collect.pop(0)
            url = url_obj['url']
            current_depth = url_obj['current_depth']

            self.dataset.update_progress(len(collected_urls) / num_urls) # annoyingly a moving target but not sure how to truly estimated it
            if depth == 0:
                self.dataset.update_status(f"Collecting {url} (URL {urls_collected} of {num_urls} collected)" )
            else:
                self.dataset.update_status(f"Collecting {url} (Depth {current_depth + 1} of {depth + 1}; URL {urls_collected} of {num_urls} collected)")

            try:
                asin_id = AmazonProductSearch.extract_asin_from_url(url)
            except ValueError:
                self.dataset.log("Unable to identify Amazon product ID (ASIN) for %s; is this a proper Amazon link?; using URL hash for ID" % url)
                asin_id = None

            result = {
                "id": asin_id if asin_id else url_to_hash(url),
                "url": url,
                "final_url": None,
                "product_id": asin_id,
                "title": None,
                "subtitle": None,
                "byline": None,
                "num_reviews": None,
                "rating": None,
                "badges": None,
                "thumbnail": None,
                "body": None,
                "html": None,
                "recommendations": {},
                "detected_404": None,
                "timestamp": None,
                "error": '',
            }

            try:
                # Get the URL
                success, errors = self.get_with_error_handling(url, max_attempts=2)

                # Collection timestamp
                result['timestamp'] = int(datetime.datetime.now().timestamp())

                # Add to collected URLs
                collected_urls.add(url)

                # Check for 404 or other errors
                detected_404 = self.check_for_404()
                if detected_404:
                    result['error'] += self.driver.title.lower() + "\n"
                    success = False
                if not success:
                    result['error'] += "; ".join(errors)
                    result['detected_404'] = detected_404
                    self.dataset.log(f"Failed to collect {url}: {result['error']}")
                    yield result
                    continue

                # Success; collect the final URL and load full page
                result["final_url"] = self.driver.current_url
                self.scroll_down_page_to_load(max_time=5)

                # Check for potential detection
                if any([detected_text in self.driver.page_source for detected_text in ["Enter the characters you see below", "Sorry, we just need to make sure you're not a robot."]]):
                    if consecutive_captchas < 5:
                        consecutive_captchas += 1
                        result['error'] += "CAPTCHA detected\n"
                        potential_captcha += 1
                        time.sleep(5)
                        if url_obj["retries"] < 1:
                            # Only retry once for CAPTCHAs
                            self.dataset.log(f"Detected potential CAPTCHA on {url}; waiting and trying again later")
                            urls_to_collect += [{'url': url, 'current_depth': current_depth, "retries": url_obj["retries"] + 1}]
                        else:
                            self.dataset.log(f"Detected CAPTCHAs multiple times on {url}; skipping")
                            result['error'] += "Too many retries\n"
                            yield result
                            continue
                    else:
                        # Too many consecutive captchas; end collection
                        result['error'] += "CAPTCHA detected\n"
                        yield result
                        self.dataset.update_status(f"Too many consecutive CAPTCHAs detected; unable to continue", is_final=True)
                        break
                else:
                    consecutive_captchas = 0

                # Collect the product details
                # These may change or not exist, but I would prefer to still collect them here if possible as we lose access to selenium later
                # We can attempt to update them from the source via map_item later (e.g., if None, check the html)
                title = self.driver.find_elements(By.XPATH, "//span[contains(@id, 'productTitle')]")
                if title:
                    result['title'] = title[0].text
                subtitle = self.driver.find_elements(By.XPATH, "//span[contains(@id, 'productSubtitle')]")
                if subtitle:
                    result['subtitle'] = subtitle[0].text
                byline = self.driver.find_elements(By.XPATH, "//div[contains(@id, 'bylineInfo')]")
                if byline:
                    result["byline"] = byline[0].text
                num_reviews = self.driver.find_elements(By.XPATH, "//a[contains(@id, 'acrCustomerReviewLink')]")
                if num_reviews:
                    result["num_reviews"] = num_reviews[0].text
                rating = self.driver.find_elements(By.XPATH, "//span[contains(@id, 'acrPopover')]")
                if rating:
                    result["rating"] = rating[0].text
                # badges
                badges = self.driver.find_elements(By.XPATH, "//div[contains(@id, 'zg-badge-wrapper')]")
                if badges:
                    result["badges"] = badges[0].text
                # image
                image_containers = self.driver.find_elements(By.XPATH, "//div[contains(@id, 'imageBlock_feature_div')]")
                if image_containers:
                    for thumb in image_containers[0].find_elements(By.XPATH, ".//img"):
                        if thumb.get_attribute("class") == "a-lazy-loaded":
                            # Ignore the lazy loaded image
                            continue
                        result["thumbnail"] = thumb.get_attribute("src")
                        break

                # Collect the HTML and extract text
                result['html'] = self.driver.page_source
                result['body'] = self.scrape_beautiful_text(result['html'])

                # Collect recommendations
                carousels = self.driver.find_elements(By.CSS_SELECTOR, "div[class*=a-carousel-container]")
                found_carousels = 0
                for carousel in carousels:
                    heading = carousel.find_elements(By.XPATH, ".//*[contains(@class, 'a-carousel-heading')]") # can be h1 or h2
                    if not heading:
                        # Not a recommendation carousel
                        continue
                    # self.dataset.log("Found carousel: %s" % heading[0].text)
                    # self.dataset.log("Carousel: %s" % carousel.get_attribute("innerHTML"))
                    # self.dataset.log("Carousel: %s" % carousel.text)

                    found_carousels += 1
                    heading_text = heading[0].text
                    result["recommendations"][heading_text] = []

                    # Collect page numbers
                    current = carousel.find_element(By.XPATH, ".//span[contains(@class, 'a-carousel-page-current')]")
                    final = carousel.find_element(By.XPATH, ".//span[contains(@class, 'a-carousel-page-max')]")
                    current = int(current.text) if current.text else 1
                    final = int(final.text) if final.text else 1
                    failure_count = 0

                    while current <= final:
                        recs = carousel.find_elements(By.TAG_NAME, "li")
                        recs_to_add = []
                        stale = False
                        for rec in recs:
                            if self.interrupted:
                                raise ProcessorInterruptedException("Interrupted while collecting Amazon product recommendations")

                            rec_link = rec.find_elements(By.CSS_SELECTOR, "a[class*=a-link-normal]")
                            if rec_link:
                                try:
                                    rec_link = rec_link[0].get_attribute("href")
                                except StaleElementReferenceException:
                                    stale = True
                                    break
                                stale = 0
                                rec_html = rec.get_attribute("innerHTML")
                                rec_data = {
                                    "text": self.scrape_beautiful_text(rec_html) if rec_html else [""],
                                    "original_link": rec_link,
                                    "normalized_link": AmazonProductSearch.normalize_amazon_links(rec_link)
                                }
                                recs_to_add.append(rec_data)
                            else:
                                # blank rec; likely all recs have been collected
                                continue

                        if stale:
                            # Stale element; try again
                            failure_count += 1
                            if failure_count >= 3:
                                # Too many failures; break
                                self.dataset.log(f"Unable to collect all recommendations from carousel {heading_text} on {url}")
                                self.log.warning(f"Amazon product collector: Stale carousel element detected too many times; unable to collect\nDataset: {self.dataset.key}\nURL: {url}\nCarousel: {heading_text}")
                                break
                            continue

                        # Add recs to the list
                        result["recommendations"][heading_text] += recs_to_add
                        # Add carousels to the list of known carousels
                        if heading_text not in known_carousels and heading_text not in added_carousels:
                            known_carousels.append(heading_text)
                            added_carousels.add(heading_text)
                            # Update the db
                            self.config.set("cache.amazon.carousels", known_carousels)

                        # Check if there is a next page and click if so
                        next_button = carousel.find_elements(By.XPATH, ".//div[contains(@class, 'a-carousel-right')]")
                        if next_button and current < final:
                            click_success = self.smart_click(next_button[0].find_element(By.XPATH, ".//span[contains(@class, 'a-button-inner')]"))
                            if not click_success:
                                self.dataset.log(f"Unable to click next button for carousel {heading_text} on {url}; continuing to next carousel")
                                break
                            try:
                                WebDriverWait(carousel, 10).until(EC.text_to_be_present_in_element(
                                    (By.XPATH, ".//span[contains(@class, 'a-carousel-page-current')]"), str(current + 1)))
                            except TimeoutException:
                                # Timeout; no new content loaded
                                self.dataset.log(f"Timeout waiting for carousel {heading_text} to load page {current + 1} on {url}; continuing to next carousel")
                                break
                            # even with the Wait for page to update, the actual recs may take a bit longer
                            time.sleep(.5)

                        current += 1

            except InvalidSessionIdException as e:
                if url_obj["retries"] > 3:
                    self.dataset.log(f"Firefox error; too many retries; skipping {url}\n{traceback.extract_stack()}\n{e}")
                    self.dataset.log(f"DEBUG: {result.get('body', '')}")
                    result['error'] += "Too many retries\n"
                    yield result
                else:
                    self.dataset.log(f"Firefox error; restarting browser and trying again\n{traceback.extract_stack()}\n{e}")
                    url_obj["retries"] += 1
                    urls_to_collect.insert(0, url_obj)

                self.restart_selenium()
                continue


            if found_carousels == 0:
                # No carousels found, but some were present
                result['error'] += "No recommendations found on page\n"
                if len(carousels) > 0:
                    # Carousels were present, but none were recommendations... possible issue w/ carousel detection
                    self.dataset.log(f"No recommendations found on {url}; unable to extract from carousels ({len(carousels)} detected)")
                    missing_carousels += 1

            if depth > 0 and result["recommendations"] and current_depth < depth:
                # Collect additional subpages
                additional_subpages = []
                if "All Recommendations" in subpage_types:
                    # Collect all types
                    for rec_links in result["recommendations"].values():
                        additional_subpages += rec_links
                else:
                    for rec_type in subpage_types:
                        if rec_type in result["recommendations"]:
                            additional_subpages += result["recommendations"][rec_type]

                # Remove duplicates
                additional_subpages = set([rec["normalized_link"] for rec in additional_subpages])
                num_urls += len(additional_subpages)
                self.dataset.update_status(f"Adding {len(additional_subpages)} additional subpages to collect")
                urls_to_collect += [{'url': url, 'current_depth': current_depth + 1, "retries":0} for url in additional_subpages if url not in collected_urls]

            urls_collected += 1
            yield result

        if missing_carousels > 0 or potential_captcha > 0:
            self.dataset.update_status(f"CAPTCHAs detected on {potential_captcha} URLs and {missing_carousels} URLs missing recommendations; see error column and log for details", is_final=True)
            if potential_captcha != missing_carousels:
                # Not a CAPTCHA issue; just missing carousels and that's odd
                if num_urls > 0 and missing_carousels/num_urls > 0.10:
                    self.log.warning(f"Amazon product collector ({self.dataset.key}): {int((missing_carousels/num_urls) * 100)}% of URLs missing recommendations")

    @staticmethod
    def map_item(page_result):
        """
        Map the item to the expected format for 4CAT

        :param json page_result:  Object with original datatypes
        :return dict:  Dictionary in the format expected by 4CAT
        """
        if not page_result.get("id"):
            page_result["id"] = page_result.get("product_id") if page_result.get("product_id") else url_to_hash(page_result.get("url"))
        # Convert the recommendations to comma-separated strings
        recommendations = page_result.pop("recommendations")
        page_result["rec_types_displayed"] = ", ".join(recommendations.keys())
        # Add known carousels as columns
        page_result["All Recommendations"] = ""
        known_carousels = config.get("cache.amazon.carousels", [])
        [page_result.update({column_name: ""}) for column_name in known_carousels]
        rec_type = None

        # Replace commas in the title; this is annoying but most of our processors simply split on commas instead of taking advantage of JSONs and lists
        if page_result.get("title"):
            page_result["title"] = page_result.get("title").replace(",", " ")

        for column_name, rec_group in recommendations.items():
            if rec_type is None and rec_group:
                rec_type = type(rec_group[0])

            def _get_rec_title(rec):
                """
                Helper function to get the title of a recommendation from the object collected

                NOTE: using rec_type defined immediately above via loop.
                """
                if rec_type is str:
                    # These are the links; originally all that was collected
                    return rec
                else:
                    rec_text = rec.get("text", [""])
                    # First item is normally the title
                    first_text = rec_text[0].replace(",", " ")
                    if len(rec_text) > 1:
                        if first_text == "Feedback":
                            # If it is "Feedback", the second item is the title
                            return rec_text[1].replace(",", " ")
                        if first_text == "Videos for this product":
                            # Video rec; the third item is the title
                            return rec_text[2].replace(",", " ")
                    return first_text

            # Add to All Recommendations column
            page_result["All Recommendations"] += (", ".join(map(_get_rec_title, rec_group)) + "; ")
            if column_name in known_carousels:
                # We cannot add all the recommendation groups as columns as they are dynamic and may change by item
                page_result[column_name] = ", ".join(map(_get_rec_title, rec_group))

        # Remove the HTML; maybe should only do for frontend...
        page_result.pop("html")

        # Convert the body to a single string
        page_result["body"] = "\n".join(page_result["body"]) if page_result["body"] else ""

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

        # this is the bare minimum, else we can't narrow down the full data set
        if not query.get("query", None):
            raise QueryParametersException("Please provide a List of urls.")
        urls = [url.strip() for url in query.get("query", "").replace("\n", ",").split(',')]
        preprocessed_urls = [url for url in urls if is_url(url)]
        if not preprocessed_urls:
            raise QueryParametersException("No Urls detected!")

        return {
            "urls": preprocessed_urls,
            "depth": query.get("depth", 0),
            "rec_type": query.get("rec_type", [])
            }

    @staticmethod
    def normalize_amazon_links(link):
        """
        Helper to remove reference information from Amazon links to standardize and ensure we do not re-collect the same
        product across different links.
        """
        link = unquote(link)
        if 'https://www.amazon.com/sspa/click?' in link:
            # Special link; remove the click reference
            parsed_url = urlparse(link)
            normal_path = parse_qs(parsed_url.query).get("url", [""])[0]
            parsed_url = parsed_url._replace(query="", path=normal_path)
        else:
            parsed_url = urlparse(link)

        parsed_url = parsed_url._replace(query="")
        path = parsed_url.path

        if "/dp/" in path:
            asin = path.split("/dp/")[1].split("/")[0]
        else:
            # Not a product link; return the original
            return link
        parsed_url = parsed_url._replace(path=f"/dp/{asin}/")

        return parsed_url.geturl()

    @staticmethod
    def extract_asin_from_url(link):
        """
        Helper to remove reference information from Amazon links to create networks
        """
        link = unquote(link)
        if "/dp/" not in link:
            raise ValueError("Unable to identify Amazon product ID (ASIN)")
        return link.split("/dp/")[1].split("/")[0]