"""
Web Archives HTML Scraper

Currently designed around Firefox, but can also work with Chrome; results may vary
"""
from urllib.parse import urlparse
import datetime
import requests
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from ural import is_url
from dateutil.relativedelta import relativedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from extensions.web_studies.selenium_scraper import SeleniumSearch
from common.lib.exceptions import QueryParametersException, ProcessorInterruptedException, ProcessorException, QueryNeedsExplicitConfirmationException
from common.lib.item_mapping import MappedItem
from common.lib.user_input import UserInput
from common.lib.helpers import url_to_hash


class SearchWebArchiveWithSelenium(SeleniumSearch):
    """
    Get HTML page source from Web Archive (web.archive.org) via the Selenium webdriver and Firefox browser
    """
    type = "web_archive_scraper-search"  # job ID
    title = "Web Archive HTML Scraper"
    category = "Search"  # category
    description = "Scrape HTML source code from the Web Archive (web.archive.org) using Selenium and Firefox"
    extension = "ndjson"

    web_archive_url = 'https://web.archive.org/web/'

    # Web Archive returns "internal error" sometimes even when snapshot exists; we retry
    bad_response_text = [
        'This snapshot cannot be displayed due to an internal error', 
        'The Wayback Machine requires your browser to support JavaScript',
        'Application error: a client-side exception has occurred (see the browser console for more information)'
        ]
    # Web Archive will load and then redirect after a few seconds; check for new page to load
    redirect_text = ['Got an HTTP 302 response at crawl time', 'Got an HTTP 301 response at crawl time']

    urls_to_exclude = ['mailto:', 'javascript', 'archive.org/about', 'archive.org/account/']

    @classmethod
    def get_options(cls, parent_dataset=None, config=None):
        options = {
            "intro-1": {
                "type": UserInput.OPTION_INFO,
                "help": "Collect HTML source code from the Web Archive (web.archive.org) using Selenium and Firefox. By setting a frequency, you can collect multiple snapshots of the same page over time."
            },
            "query-info": {
                "type": UserInput.OPTION_INFO,
                "help": "Please enter a list of urls one per line."
            },
            "query": {
                "type": UserInput.OPTION_TEXT_LARGE,
                "help": "List of urls"
            },
            "frequency": {
                "type": UserInput.OPTION_CHOICE,
                "help": "Frequency over time period",
                "tooltip": "Default 'First Available' scrapes the first available result after start date",
                "options": {
                    "first": "First Available",
                    "monthly": "Monthly",
                    "weekly": "Weekly",
                    "daily": "Daily",
                    "yearly": "Yearly"
                },
                "default": "first"
            },
            "daterange": {
                "type": UserInput.OPTION_DATERANGE,
                "tooltip": "Scrapes first available page after start date; Uses start and end date for frequency",
                "help": "Date range"
            },
        }

        if config.get("selenium.display_advanced_options", default=False):
            options["http_request"] = {
                "type": UserInput.OPTION_CHOICE,
                "help": "HTTP or Selenium request",
                "tooltip": "Scrape data with HTTP (python request library) and/or Selenium (automated browser to better imitate a real user); HTTP response is added to body field, but not currently parsed to extract text",
                "options": {
                    "both": "Both HTTP request and Selenium WebDriver",
                    "selenium_only": "Only use Selenium WebDriver",
                },
                "default": "selenium_only"
            }

        return options

    @staticmethod
    def request_available_archive_urls(url, min_date=None, max_date=None, limit=None):
        """
        Get available Archive.org snapshots within a timeframe for given URL.

        API docs:
        https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server#basic-usage

        :param url: The URL to check for archived snapshots.
        :param min_date: The minimum date for the snapshot search.
        :param max_date: The maximum date for the snapshot search.
        :param frequency: The frequency of snapshots to retrieve.
        :param limit: The maximum number of snapshots to retrieve. (positive integer start from beginning, negative from end)
        """
        if min_date is None and limit is None:
            # Technically not true but results can be massive
            raise ProcessorException("Either min_date or limit must be provided to search for archived URLs.")
        if url.startswith("http"):
            # API does not accept http(s):// URLs
            url = "://".join(url.split("://")[1:])
        url_params = {
            "url": url,
            "output": "json",
            "filter": "statuscode:200",
        }
        if min_date is not None:
            url_params["from"] = datetime.datetime.fromtimestamp(int(min_date)).strftime('%Y%m%d%H%M%S')
        if max_date is not None:
            url_params["to"] = datetime.datetime.fromtimestamp(int(max_date)).strftime('%Y%m%d%H%M%S')
        if limit is not None:
            url_params["limit"] = limit
            if limit < 0:
                url_params["fastLatest"] = "true"

        # Use HTTPS and a Session with retries/backoff for transient network errors
        cdx_url = "https://web.archive.org/cdx/search/cdx"
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(['GET', 'HEAD', 'OPTIONS'])
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        # If your environment requires proxies, allow requests to read env vars:
        session.trust_env = True
        try:
            response = session.get(cdx_url, params=url_params, timeout=60)
        except requests.exceptions.ConnectionError as e:
            raise ProcessorException(f"Connection error to Archive.org CDX API: {e}")
        except requests.exceptions.RequestException as e:
            raise ProcessorException(f"Error requesting Archive.org CDX API: {e}")

        if response.status_code == 200:
            try:
                return response.json()
            except ValueError as e:
                raise ProcessorException(f"Invalid JSON from Archive.org CDX API: {e}")
        else:
            raise ProcessorException(f"Error {response.status_code} from Archive.org CDX server: {response.text[:200]}")
        
    @staticmethod
    def create_web_archive_url(date, url):
        """
        Create a Web Archive URL for a specific date and original URL.

        :param date: The date of the archived snapshot.
        :param url: The original URL to be archived.
        :return: The Web Archive URL.
        """
        date_str = date.strftime('%Y%m%d%H%M%S')
        return f"http://web.archive.org/web/{date_str}/{url}"
    
    @staticmethod
    def check_web_archive_page_loaded(driver):
        """
        Check if the Web Archive page has fully loaded.
        """
        try:
            # Wait for DOM ready and presence of <body>
            WebDriverWait(driver, 30).until(
                lambda d: d.execute_script('return document.readyState') == 'complete'
            )
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            # TODO: revisit
            # Try to wait for meaningful content: main/article/role=main or hydrated #__next children
            # content_ready = False
            # try:
            #     WebDriverWait(driver, 10).until(
            #         EC.presence_of_element_located((By.CSS_SELECTOR, "main, article, [role='main'], #__next > *"))
            #     )
            #     content_ready = True
            # except TimeoutException:
            #     content_ready = False
            # if not content_ready:
            #     return False
        except TimeoutException:
            return False

        # Quick sanity checks against known IA error pages
        page_source = driver.page_source or ""
        if any(bad in page_source for bad in SearchWebArchiveWithSelenium.bad_response_text):
            return False

        return True

    @staticmethod
    def extract_web_archive_content(driver):
        """
        Extract the main content from a Web Archive page.
        """
        # Wait for the body to appear (defensive)
        try:
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        except TimeoutException:
            # Proceed anyway; we'll get whatever is available
            pass

        final_url = driver.current_url
        page_title = driver.title
        page_source = driver.page_source or ""

        # Visible text via BeautifulSoup helper in Selenium stack
        body_text_list = []
        try:
            body_text_list = SeleniumSearch.scrape_beautiful_text(page_source)
        except Exception:
            body_text_list = []

        # Links via BeautifulSoup for stable absolute URLs
        try:
            parsed = urlparse(final_url)
            domain = f"{parsed.scheme}://{parsed.netloc}"
            _, bs_links = SeleniumSearch.get_beautiful_links(page_source, domain)
        except Exception:
            bs_links = []

        # Also collect raw hrefs via Selenium (can include archive prefixes)
        try:
            hrefs = [el.get_attribute('href') for el in driver.find_elements(By.XPATH, "//a[@href]")]
        except Exception:
            hrefs = []

        return {
            'final_url': final_url,
            'page_title': page_title,
            'page_source': page_source,
            'text': body_text_list,
            'links': hrefs,
            'scraped_links': bs_links,
        }

    @staticmethod
    def build_segments(start_ts, end_ts, freq):
        """
        Build time segments based on the specified frequency.
        """
        start_dt = datetime.datetime.fromtimestamp(int(start_ts)) if start_ts is not None else None
        end_dt = datetime.datetime.fromtimestamp(int(end_ts)) if end_ts is not None else None
        if start_dt is None and end_dt is None:
            return []
        if start_dt is None:
            # default to 1 year before end if only end provided
            start_dt = end_dt - relativedelta(years=1)
        if end_dt is None:
            end_dt = datetime.datetime.now()

        segments = []
        cur = start_dt
        if freq == 'first':
            segments.append((start_dt, end_dt))
        elif freq == 'yearly':
            while cur <= end_dt:
                seg_start = datetime.datetime(cur.year, 1, 1)
                seg_end = datetime.datetime(cur.year, 12, 31, 23, 59, 59)
                # clamp to bounds
                if seg_end < start_dt:
                    cur = seg_end + datetime.timedelta(seconds=1)
                    continue
                segments.append((max(seg_start, start_dt), min(seg_end, end_dt)))
                cur = seg_end + datetime.timedelta(seconds=1)
        elif freq == 'monthly':
            while cur <= end_dt:
                seg_start = datetime.datetime(cur.year, cur.month, 1)
                seg_end = seg_start + relativedelta(months=1) - datetime.timedelta(seconds=1)
                segments.append((max(seg_start, start_dt), min(seg_end, end_dt)))
                cur = seg_end + datetime.timedelta(seconds=1)
        elif freq == 'weekly':
            # Use 7-day windows from start_dt
            cur = start_dt
            while cur <= end_dt:
                seg_start = cur
                seg_end = cur + datetime.timedelta(days=7) - datetime.timedelta(seconds=1)
                segments.append((max(seg_start, start_dt), min(seg_end, end_dt)))
                cur = seg_end + datetime.timedelta(seconds=1)
        elif freq == 'daily':
            cur = start_dt
            while cur <= end_dt:
                seg_start = datetime.datetime(cur.year, cur.month, cur.day)
                seg_end = seg_start + datetime.timedelta(days=1) - datetime.timedelta(seconds=1)
                segments.append((max(seg_start, start_dt), min(seg_end, end_dt)))
                cur = seg_end + datetime.timedelta(seconds=1)
        else:
            raise ProcessorException(f"Frequency {freq} not implemented!")

        return segments

    def iter_snapshots(self, query):
        """
        Central iterator that yields one event per segment:
        - On success: {'ok': True, 'url', 'seg_start', 'dt', 'snapshot_url'} after a snapshot has been loaded.
        - On failure: {'ok': False, 'url', 'seg_start', 'fail_url', 'error'} when no snapshot succeeded in a segment.

    This method also updates dataset progress/status per segment to avoid duplication in subclasses.
    """
        min_date = query.get('min_date')
        max_date = query.get('max_date')
        frequency = query.get('frequency')

        # Precompute total segments for progress
        total_segments = 0
        url_to_segments = {}
        for url in query.get('validated_urls'):
            url_to_segments[url] = self.build_segments(min_date, max_date, frequency)
            total_segments += len(url_to_segments[url])

        completed_segments = 0

        for url in query.get('validated_urls'):
            # If user supplied a specific Web Archive URL, just attempt to load it directly once
            if self.web_archive_url == url[:len(self.web_archive_url)]:
                segments = [(datetime.datetime.fromtimestamp(min_date) if min_date else None,
                             datetime.datetime.fromtimestamp(max_date) if max_date else None)]
                captures = [[None, url.split('/web/')[1].split('/')[0], url]]  # mimic CDX row
            else:
                # Fetch available captures for this URL
                limit = None if min_date is not None else -5
                try:
                    captures_json = self.request_available_archive_urls(url, min_date=min_date, max_date=max_date, limit=limit)
                except Exception as e:
                    self.dataset.log(f"Unable to reach Web Archive API: {url} - {str(e)}")
                    # Yield a failure event so callers can record/report the failure
                    yield {
                        'ok': False,
                        'url': url,
                        'seg_start': None,
                        'fail_url': None,
                        'error': f"WebArchive API error: {str(e)}",
                    }
                    continue

                if not captures_json or len(captures_json) <= 1:
                    self.dataset.log(f"No archived snapshots found for {url} in given range")
                    # Yield a failure event so callers can record/report the missing snapshots
                    yield {
                        'ok': False,
                        'url': url,
                        'seg_start': None,
                        'fail_url': None,
                        'error': 'No archived snapshots found in given range',
                    }
                    continue

                # Normalize and sort captures by timestamp asc
                _, rows = captures_json[0], captures_json[1:]
                try:
                    rows.sort(key=lambda r: r[1])
                except Exception:
                    pass
                captures = rows
                segments = url_to_segments[url]

            # Iterate segments and try captures inside each
            for seg_start, seg_end in segments:
                if self.interrupted:
                    raise ProcessorInterruptedException("Interrupted while scraping urls from the Web Archive")

                # Filter captures within segment window
                seg_caps = []
                for r in captures:
                    ts = r[1]
                    try:
                        dt = datetime.datetime.strptime(ts, '%Y%m%d%H%M%S')
                    except Exception:
                        continue
                    if (seg_start is None or dt >= seg_start) and (seg_end is None or dt <= seg_end):
                        seg_caps.append((dt, r))

                # For 'first', reduce to the earliest capture after start
                if frequency == 'first':
                    seg_caps = seg_caps[:1] if seg_caps else []

                # Attempt each capture in this segment until one loads properly
                segment_success = False
                segment_error = ''

                for dt, r in seg_caps:
                    ts = r[1]
                    original_url = r[2] if len(r) > 2 else url
                    snapshot_url = f"http://web.archive.org/web/{ts}/{original_url}"

                    # Try to navigate
                    success, errors = self.get_with_error_handling(snapshot_url, max_attempts=2, wait=2, restart_browser=True)
                    if not success:
                        segment_error += ('\n'.join([str(e) for e in errors]) + '\n') if errors else ''
                        continue
                    
                    # Scroll to bottom of page to load
                    self.scroll_down_page_to_load(max_time=30)

                    # Wait for load and handle possible redirect within archive playback
                    if not self.check_page_is_loaded(max_time=60):
                        segment_error += f"Timeout loading {snapshot_url}\n"
                        continue

                    # If Wayback reports redirect, wait briefly for movement
                    try:
                        page_text = SeleniumSearch.scrape_beautiful_text(self.driver.page_source)
                    except Exception:
                        page_text = []
                    if any(any(rt in t for rt in self.redirect_text) for t in page_text):
                        # give it a short window to redirect
                        last_url = self.driver.current_url
                        end_wait = time.time() + 10
                        moved = False
                        while time.time() < end_wait:
                            time.sleep(1)
                            if self.driver.current_url != last_url:
                                moved = True
                                break
                        if moved:
                            # Re-check load
                            self.scroll_down_page_to_load(max_time=30)
                            if not self.check_page_is_loaded(max_time=30):
                                segment_error += f"Redirected but final page did not load for {snapshot_url}\n"
                                continue

                    # Final validation against IA internal error pages
                    if not self.check_web_archive_page_loaded(self.driver):
                        segment_error += f"Archive page not properly loaded for {snapshot_url}\n"
                        continue

                    # Success for this segment
                    segment_success = True
                    # Progress update here to centralize
                    completed_segments += 1
                    if total_segments:
                        self.dataset.update_progress(completed_segments / total_segments)
                    self.dataset.update_status(f"Captured {completed_segments} of {total_segments} possible segments")
                    yield {
                        'ok': True,
                        'url': url,
                        'seg_start': seg_start,
                        'dt': dt,
                        'snapshot_url': snapshot_url,
                    }
                    break

                if not segment_success:
                    # Progress update for failed segment
                    completed_segments += 1
                    if total_segments:
                        self.dataset.update_progress(completed_segments / total_segments)
                    self.dataset.update_status(f"Captured {completed_segments} of {total_segments} possible segments")

                    fail_url = f"{self.web_archive_url}{seg_start.strftime('%Y%m%d%H%M%S') if seg_start else ''}/{url}"
                    yield {
                        'ok': False,
                        'url': url,
                        'seg_start': seg_start,
                        'fail_url': fail_url,
                        'error': segment_error if segment_error else 'No successful snapshot in segment',
                    }

    def get_items(self, query):
        """
        Separate and check urls, then loop through each and collects the HTML.

        :param query:
        :return:
        """
        http_request = self.parameters.get("http_request", "selenium_only") == 'both'
        if http_request:
            self.dataset.update_status('Scraping Web Archives with Selenium %s and HTTP Requests' % self.browser)
        else:
            self.dataset.update_status('Scraping Web Archives with Selenium %s' % self.browser)

        failures = 0
        successes = 0
        for event in self.iter_snapshots(query):
            if event.get('ok'):
                successes += 1
                # Extract content and yield an item (HTML scraper behavior)
                content = self.extract_web_archive_content(self.driver)
                dt = event['dt']
                seg_start = event['seg_start']
                snapshot_url = event['snapshot_url']
                base_url = event['url']

                result = {
                    "id": url_to_hash(snapshot_url),
                    "base_url": base_url,
                    "year": (seg_start.year if seg_start else dt.year),
                    "date": dt.strftime('%Y-%m-%d %H:%M:%S') if dt else None,
                    "url": snapshot_url,
                    "final_url": content.get('final_url'),
                    "subject": content.get('page_title'),
                    "body": content.get('text'),
                    "html": content.get('page_source'),
                    "http_html": None,
                    "detected_404": self.check_for_404(),
                    "timestamp": int(datetime.datetime.now().timestamp()),
                    "error": '',
                    "selenium_links": content.get('links'),
                }

                # Collect links from page source for export
                result['scraped_links'] = content.get('scraped_links')

                # Optional HTTP request of final_url
                if http_request and result.get('final_url'):
                    try:
                        http_response = self.request_get_w_error_handling(result.get('final_url'), timeout=120)
                        self.dataset.log(f"Collected HTTP response: {result.get('final_url')}")
                        result['http_html'] = http_response.text
                    except Exception as e:
                        result['http_html'] = None
                        result['error'] += ('\nHTTP ERROR:\n' + str(e))

                yield result
            else:
                failures += 1
                seg_start = event['seg_start']
                fail_url = event['fail_url']
                datetimestr = seg_start.strftime('%Y-%m-%d %H:%M:%S') if seg_start else None
                self.dataset.update_status(f"Failed snapshot: {event['url']}{' @ ' + datetimestr if datetimestr else ''}")
                self.dataset.log(f"Failed snapshot: {event['url']}{' @ ' + datetimestr if datetimestr else ''} - {event.get('error', '')}")
                yield {
                    "id": url_to_hash(fail_url),
                    "base_url": event['url'],
                    "year": seg_start.year if seg_start else None,
                    "date": datetimestr,
                    "url": fail_url,
                    "final_url": None,
                    "subject": None,
                    "body": None,
                    "html": None,
                    "http_html": None,
                    "detected_404": None,
                    "timestamp": int(datetime.datetime.now().timestamp()),
                    "error": event.get('error', ''),
                    "selenium_links": [],
                    "scraped_links": [],
                }

        if failures:
            self.dataset.update_status(f"Completed with {successes} screenshots and {failures} failures; see log for details", is_final=True)
        else:
            self.dataset.update_status(f"Completed with {successes} screenshots and no failures")


    def request_get_w_error_handling(self, url, retries=3, **kwargs):
        """
        Try requests.get() and logging error in dataset.log().

        Retries ConnectionError three times by default
        """
        try:
            response = requests.get(url, **kwargs)
        except requests.exceptions.Timeout as e:
            self.dataset.log("Error: Timeout on url %s: %s" % (url, str(e)))
            raise e
        except requests.exceptions.SSLError as e:
            self.dataset.log("Error: SSLError on url %s: %s" % (url, str(e)))
            raise e
        except requests.exceptions.TooManyRedirects as e:
            self.dataset.log("Error: TooManyRedirects on url %s: %s" % (url, str(e)))
            raise e
        except requests.exceptions.ConnectionError as e:
            if retries > 0:
                response = self.request_get_w_error_handling(url, retries=retries - 1, **kwargs)
            else:
                self.dataset.log("Error: ConnectionError on url %s: %s" % (url, str(e)))
                raise e
        return response

    @staticmethod
    def map_item(page_result):
        """
        Map webpage result from JSON to 4CAT expected values.

        This makes some minor changes to ensure processors can handle specific
        columns and "export to csv" has formatted data.

        :param json page_result:  Object with original datatypes
        :return dict:  Dictionary in the format expected by 4CAT
        """
        # Convert list of text strings to one string
        page_result['body'] = '\n'.join(page_result.get('body')) if page_result.get('body') else ''
        # Convert list of link objects to comma separated urls
        page_result['scraped_links'] = ','.join([link.get('url') for link in page_result.get('scraped_links')]) if page_result.get('scraped_links') else ''
        # Convert list of links to comma separated urls
        page_result['selenium_links'] = ','.join(map(str, page_result.get('selenium_links'))) if isinstance(page_result.get('selenium_links'), list) else page_result.get('selenium_links', '')

        return MappedItem(page_result)

    @staticmethod
    def create_web_archive_urls(url, start_date, end_date, frequency):
        """
        Combines url with Web Archive base (https://web.archive.org/web/) if
        needed along with start date to create urls. Will use frequency to
        create additional urls if needed.

        :param str url: url as string
        :param start_date: starting date
        :param end_date: ending date
        :param string frequency: frequency of scrape
        :return list: List of urls to scrape
        """
        web_archive_url = 'https://web.archive.org/web/'
        min_date = datetime.datetime.fromtimestamp(int(start_date))
        max_date = datetime.datetime.fromtimestamp(int(end_date))

        # if already formated, return as is
        if web_archive_url == url[:len(web_archive_url)]:
            return [{'base_url': url, 'year': min_date.year, 'url': url}]

        if frequency == 'yearly':
            years = [year for year in range(min_date.year, max_date.year+1)]

            return  [
                     {
                     'base_url': url,
                     'year': year,
                     'url': web_archive_url + str(year) + min_date.strftime('%m%d') + '/' + url,
                     }
                    for year in years]

        elif frequency in ('monthly', 'weekly', 'daily'):
            dates_needed = []
            current = min_date
            while current <= max_date:
                dates_needed.append({
                     'base_url': url,
                     'year': current.year,
                     'url': web_archive_url + current.strftime('%Y%m%d') + '/' + url,
                     })
                if frequency == 'weekly':
                    current += relativedelta(weeks=1)
                elif frequency == 'monthly':
                    current += relativedelta(months=1)
                elif frequency == 'daily':
                    current += relativedelta(days=1)
                else:
                    raise ProcessorException("Frequency %s not implemented!" % frequency)

            return dates_needed

        elif frequency == 'first':
            return [{'base_url': url, 'year': min_date.year, 'url': web_archive_url + min_date.strftime('%Y%m%d') + '/' + url}]

        else:
            raise Exception('frequency type %s not implemented!' % frequency)

    @staticmethod
    def validate_query(query, request, config):
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

        urls = [url.strip() for url in query.get("query", "").split('\n')]
        validated_urls = [url for url in urls if is_url(url)]
        if not validated_urls:
            raise QueryParametersException("No Urls detected!")

        # the dates need to make sense as a range to search within
        query["min_date"], query["max_date"] = query.get("daterange")
        if query["max_date"] is None:
            query["max_date"] = int(datetime.datetime.now().timestamp())
        if query["min_date"] is None:
            raise QueryParametersException("Please provide a start date.")
        if query["max_date"] < query["min_date"]:
            raise QueryParametersException("End date must be after start date.")
        
        if not query.get("frontend-confirm"):
            # Estimate max possible snapshots to avoid overload
            frequency = query.get("frequency", "first")
            segments = SearchWebArchiveWithSelenium.build_segments(query["min_date"], query["max_date"], frequency)
            if len(segments) > 50:
                warning = "This query requests %s snapshots per URL" % ("{:,}".format(len(segments)))
                warning += " Do you want to continue?"
                raise QueryNeedsExplicitConfirmationException(warning)

        return {
            "query": query.get("query"),
            "min_date": query.get("min_date"),
            "max_date": query.get("max_date"),
            "frequency": query.get("frequency"),
            "validated_urls": validated_urls,
            "http_request": query.get("http_request", "selenium_only"),
            "pause-time": query.get("pause-time", 6)
            }
