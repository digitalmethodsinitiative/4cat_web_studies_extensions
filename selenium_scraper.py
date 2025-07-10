import subprocess
import time
import shutil
import abc
import os
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from bs4.element import Comment
from ural import is_url
from requests.utils import requote_uri

from backend.lib.search import Search
from common.lib.exceptions import ProcessorException
from common.lib.user_input import UserInput

from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import WebDriverException, SessionNotCreatedException, UnexpectedAlertPresentException, \
TimeoutException, JavascriptException, NoAlertPresentException, ElementClickInterceptedException, InvalidSessionIdException, \
ElementNotInteractableException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys


########################################################
# This is to attempt to fix a bug in Selenium's logger #
########################################################
import logging
class CustomFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, 'location'):
            record.location = 'N/A'
        return super().format(record)


class SeleniumWrapper(metaclass=abc.ABCMeta):
    """
    Selenium Scraper class

    Selenium utilizes a chrome webdriver and chrome browser to navigate and scrape the web. This processor can be used
    to initialize that browser and navigate it as needed. It replaces search to allow you to utilize the Selenium driver
    and ensure the webdriver and browser are properly closed out upon completion.
    """

    driver = None
    last_scraped_url = None
    browser = None
    eager_selenium = False
    selenium_log = None
    config = None
    _setup_done = False

    consecutive_errors = 0
    num_consecutive_errors_before_restart = 3

    def setup(self, config):
        """
        Setup the SeleniumWrapper. This injects the config object and sets up the logger.
        """
        self.config = config

        # Setup the logger
        # I would prefer to use our log class but it seems to cause issue with selenium's logger
        formatter = CustomFormatter('%(asctime)s - %(name)s - %(levelname)s - %(location)s - %(message)s')
        self.selenium_log = logging.getLogger('selenium')
        self.selenium_log.setLevel(logging.INFO)
        file_handler = logging.FileHandler(self.config.get("PATH_LOGS").joinpath('selenium.log'))
        file_handler.setFormatter(formatter)
        self.selenium_log.addHandler(file_handler)

        self._setup_done = True

    def get_with_error_handling(self, url, max_attempts=1, wait=0, restart_browser=False):
        """
        Attempts to call driver.get(url) with error handling. Will attempt to restart Selenium if it fails and can
        attempt to kill Firefox (and allow Selenium to restart) itself if allowed.

        Returns a tuple containing a bool (True if successful, False if not) and a list of the errors raised.
        """
        # Start clean
        try:
            self.reset_current_page()
        except InvalidSessionIdException:
            # Somehow we lost the session; restart Selenium
            self.restart_selenium()

        success = False
        attempts = 0
        errors = []
        while attempts < max_attempts:
            attempts += 1
            try:
                self.driver.get(url)
                success = True
                self.consecutive_errors = 0
            except TimeoutException as e:
                errors.append(f"Timeout retrieving {url}: {e}")
            except Exception as e:
                self.selenium_log.error(f"Error driver.get({url}){(' (dataset '+self.dataset.key+') ') if hasattr(self, 'dataset') else ''}: {e}")
                errors.append(e)
                self.consecutive_errors += 1
                
                # Check consecutive errors
                if self.consecutive_errors > self.num_consecutive_errors_before_restart:
                    # First kill browser
                    if restart_browser:
                        self.kill_browser(self.browser)
                    
                    # Then restart Selenium
                    self.restart_selenium()

            if success:
                # Check for movement
                if self.check_for_movement():
                    # True success
                    break
                else:
                    success = False
                    errors.append(f"Failed to navigate to new page (current URL: {self.last_scraped_url}); check url is not the same as previous url")

            if attempts < max_attempts:
                time.sleep(wait)

        return success, errors

    def simple_scrape_page(self, url, extract_links=False, title_404_strings='default'):
        """
        Simple helper to scrape url. Returns a dictionary containing basic results from scrape including final_url,
        page_title, and page_source otherwise False if the page did not advance (self.check_for_movement() failed).
        Does not handle errors from driver.get() (e.g., badly formed URLs, Timeouts, etc.).

        Note: calls self.reset_current_page() prior to requesting url to ensure each page is uniquely checked.

        You are invited to use this as a template for more complex scraping.

        :param str url:  url as string; beginning with scheme (e.g., http, https)
        :param List title_404_strings:  List of strings representing possible 404 text to be compared with driver.title
        :return dict: A dictionary containing basic results from scrape including final_url, page_title, and page_source.
                      Returns false if no movement was detected
        """

        self.reset_current_page()
        self.driver.get(url)

        if self.check_for_movement():

            results = self.collect_results(url, extract_links, title_404_strings)
            return results

        else:
            raise Exception("Failed to navigate to new page; check url is not the same as previous url")

    def collect_results(self, url, extract_links=False, title_404_strings='default'):

        result = {
            'original_url': url,
            'detected_404': self.check_for_404(title_404_strings),
            'page_title': self.driver.title,
            'final_url': self.driver.current_url,
            'page_source': self.driver.page_source,
            }

        if extract_links:
            result['links'] = self.collect_links()

        return result

    def collect_links(self):
        """

        """
        if self.driver is None:
            raise ProcessorException('Selenium Drive not yet started: Cannot collect links')

        elems = self.driver.find_elements(By.XPATH, "//a[@href]")
        return [elem.get_attribute("href") for elem in elems]

    @staticmethod
    def check_exclude_link(link, previously_used_links, base_url=None, bad_url_list=None):
        """
        Check if a link should not be used. Returns True if link not in previously_used_links
        and not in bad_url_list. If a base_url is included, the link string MUST include the
        base_url as a substring (this can be used to ensure a url contains a particular domain).

        If bad_url_lists is None, the default list (['mailto:', 'javascript']) is used.

        :param str link:                    link to check
        :param set previously_used_links:   set of links to exclude
        :param str base_url:                substring to ensure is part of link
        :param list bad_url_list:           list of substrings to exclude
        :return bool:                       True if link should NOT be excluded else False
        """
        if bad_url_list is None:
            bad_url_list = ['mailto:', 'javascript']

        if link and link not in previously_used_links and \
            not any([bad_url in link[:len(bad_url)] for bad_url in bad_url_list]):
                if base_url is None:
                    return True
                elif base_url in link:
                    return True
                else:
                    return False
        else:
            return False

    def start_selenium(self, eager=None, config=None):
        """
        Start a headless browser

        :param bool eager:  Eager loading? If None, uses class attribute self.eager_selenium (default False)
        """
        import time
        start_time = time.time()
        
        # Ensure we have a config object
        if not self._setup_done:
            # config can be passed directly
            if config is not None:
                self.setup(config)
            elif self.config is not None:
                # BasicWorkers (e.g., Search) will have a config object set during `process`
                self.setup(self.config)
            else:
                raise ProcessorException("SeleniumWrapper not setup; please call setup() with a config object before starting Selenium.")

        if eager is not None:
            # Update eager loading
            self.eager_selenium = eager

        self.browser = self.config.get('selenium.browser')
        self.selenium_log.info(f"Starting Selenium with browser: {self.browser}")
        
        # Selenium options
        # TODO review and compare Chrome vs Firefox options
        if self.browser == 'chrome':
            from selenium.webdriver.chrome.options import Options
        elif self.browser == 'firefox':
            from selenium.webdriver.firefox.options import Options
        else:
            raise ImportError('selenium.browser only works with "chrome" or "firefox"')
        
        options_start = time.time()
        options = Options()
        options.headless = True
        
        if self.browser == 'firefox':
            # Firefox-specific optimizations - no profile creation for speed
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-extensions")
            options.add_argument("--private")
            
            # Set preferences directly in options to avoid profile creation
            options.set_preference("dom.webdriver.enabled", False)
            options.set_preference('useAutomationExtension', False)
            options.set_preference("browser.privatebrowsing.autostart", True)
            options.set_preference("browser.cache.disk.enable", False)
            options.set_preference("browser.cache.memory.enable", False)
            options.set_preference("permissions.default.image", 2)  # Block images for speed
        else:
            # Chrome-specific options
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument("--disable-gpu")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-browser-side-navigation")

        if self.eager_selenium:
            options.set_capability("pageLoadStrategy", "eager")
            
        options_time = time.time() - options_start
        self.selenium_log.info(f"Options setup took: {options_time:.2f}s")

        driver_start = time.time()
        try:
            if self.browser == 'chrome':
                self.driver = webdriver.Chrome(executable_path=self.config.get('selenium.selenium_executable_path'), options=options)
            elif self.browser == 'firefox':
                # Create Firefox service
                service = FirefoxService(executable_path=self.config.get('selenium.selenium_executable_path'))
                
                # Create Firefox driver with modern API (no profile needed)
                self.driver = webdriver.Firefox(service=service, options=options)
                self.driver.maximize_window() # most users browse maximized
            else:
                if hasattr(self, 'dataset'):
                    self.dataset.update_status("Selenium Scraper not configured")
                raise ProcessorException("Selenium Scraper not configured; browser must be 'firefox' or 'chrome'")
        except (SessionNotCreatedException, WebDriverException) as e:
            if hasattr(self, 'dataset'):
                self.dataset.update_status("Selenium Scraper not configured; contact admin.", is_final=True)
                self.dataset.finish(0)
            if "only supports Chrome" in str(e):
                raise ProcessorException("Your chromedriver version is incompatible with your Chromium version:\n  (%s)" % e)
            elif "Message: '' executable may have wrong" in str(e):
                raise ProcessorException('Webdriver not installed or path to executable incorrect (%s)' % str(e))
            else:
                raise ProcessorException("Could not connect to browser (%s)." % str(e))
                
        driver_time = time.time() - driver_start
        self.selenium_log.info(f"Driver creation took: {driver_time:.2f}s")
        
        # Test adding a script to remove webdriver detection
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        total_time = time.time() - start_time
        self.selenium_log.info(f"Total Selenium startup time: {total_time:.2f}s (PID: {self.driver.service.process.pid})")

    def quit_selenium(self):
        """
        Always attempt to close the browser otherwise multiple versions of Chrome will be left running.

        And Chrome is a memory hungry monster.
        """
        try:
            self.driver.quit()
        except Exception as e:
            self.selenium_log.error(e)

    def restart_selenium(self, eager=None):
        """
        Weird Selenium error? Restart and try again.
        """
        self.quit_selenium()
        self.start_selenium(eager=eager)
        self.reset_current_page()

    def set_page_load_timeout(self, timeout=60):
        """
        Adjust the time that Selenium will wait for a page to load before failing
        """
        self.driver.set_page_load_timeout(timeout)

    def check_for_movement(self):
        """
        Some driver.get() commands will not result in an error even if they do not result in updating the page source.
        This can happen, for example, if a url directs the browser to attempt to download a file. It can therefore be
        important to check and ensure a new page was actually obtained before retrieving the page source as you will
        otherwise retrieve he same information from the previous url.

        WARNING: It may also be true that a url redirects to the same url as previous scraped url. This check would assume no
        movement occurred. Use in conjunction with self.reset_current_page() if it is necessary to check every url results
        and identify redirects.
        """
        try:
            current_url = self.driver.current_url
        except UnexpectedAlertPresentException:
            # attempt to dismiss random alert
            self.dismiss_alert()
            current_url = self.driver.current_url
        if current_url == self.last_scraped_url:
            return False
        else:
            return True

    def dismiss_alert(self):
        """
        Dismiss any alert that may be present
        """
        current_window_handle = self.driver.current_window_handle
        try:
            alert = self.driver.switch_to.alert
            if alert:
                alert.dismiss()
        except NoAlertPresentException:
            return
        self.driver.switch_to.window(current_window_handle)

    def check_page_is_loaded(self, max_time=60, auto_dismiss_alert=True):
        """
        Check if page is loaded. Returns True if loaded, False if not.
        """
        try:
            try:
                WebDriverWait(self.driver, max_time).until(
                    lambda driver: driver.execute_script('return document.readyState') == 'complete')
            except UnexpectedAlertPresentException as e:
                # attempt to dismiss random alert
                if auto_dismiss_alert:
                    self.dismiss_alert()
                    WebDriverWait(self.driver, max_time).until(
                        lambda driver: driver.execute_script('return document.readyState') == 'complete')
                else:
                    raise e
        except TimeoutException:
            return False

        return True

    def reset_current_page(self):
        """
        It may be desirable to "reset" the current page, for example in conjunction with self.check_for_movement(),
        to ensure the results are obtained for a specific url provided.

        Example: driver.get(url_1) is called and page_source is collected. Then driver.get(url_2) is called, but fails.
        Depending on the type of failure (which may not be detected), calling page_source may return the page_source
        from url_1 even after driver.get(url_2) is called.
        """
        self.driver.get('data:,')
        self.last_scraped_url = self.driver.current_url

    def check_for_404(self, stop_if_in_title='default'):
        """
        Checks page title for references to 404

        Selenium does not have a "status code" in the same way the python requests and other libraries do. This can be
        used to approximate a 404. Alternately, you could use another library to check for 404 errors but that can lead
        to misleading results (as the new library will necessarily constitute a separate request).
        More information here:
        https://www.selenium.dev/documentation/worst_practices/http_response_codes/

        Default values: ["page not found", "directory not found", "file not found", "404 not found", "error 404"]

        :param list stop_if_in_title:  List of strings representing possible 404 text
        """
        if stop_if_in_title == 'default':
            stop_if_in_title = ["page not found", "directory not found", "file not found", "404 not found", "error 404", "error page"]

        if any(four_oh_four.lower() in self.driver.title.lower() for four_oh_four in stop_if_in_title):
            return True
        else:
            return False

    def enable_download_in_headless_chrome(self, download_dir):
        """
        It is possible to allow the webbrowser to download files.
        NOTE: this could introduce security risks.
        """
        # add missing support for chrome "send_command"  to selenium webdriver
        self.driver.command_executor._commands["send_command"] = ("POST", '/session/$sessionId/chromium/send_command')

        params = {'cmd': 'Page.setDownloadBehavior', 'params': {'behavior': 'allow', 'downloadPath': download_dir}}
        return self.driver.execute("send_command", params)

    def enable_firefox_extension(self, path_to_extension, temporary=True):
        """
        Enables Firefox extension.
        """
        if self.browser != 'firefox':
            raise Exception('Cannot add firefox extension to non firefox browser!')
        if self.driver is None:
            raise Exception('Must start firefox before installing extension!')
        self.driver.install_addon(os.path.abspath(path_to_extension), temporary=temporary)

    def save_screenshot(self, path, wait=2, width=None, height=None, viewport_only=False):
        # Save current screen size
        original_size = self.driver.get_window_size()
        dom_width = self.driver.execute_script('return document.body.parentNode.scrollWidth')
        dom_height = self.driver.execute_script('return document.body.parentNode.scrollHeight')

        # Wait 30 up to 30 seconds for 'body' to load
        WebDriverWait(self.driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))

        # Gather and adjust screen size
        if not width:
            width = dom_width

        if not height:
            height = dom_height

        self.driver.set_window_size(width, height)

        # Wait for page to load
        time.sleep(wait)

        # Take screenshot
        if viewport_only:
            self.driver.execute_script("return document.body.style.overflow = 'hidden';")
            self.driver.save_screenshot(str(path))  # has scrollbar
        else:
            self.driver.find_element(By.TAG_NAME, "body").screenshot(str(path))  # avoids scrollbar

        # Return to previous size (might not be necessary)
        self.driver.set_window_size(original_size['width'], original_size['height'])

    # Some BeautifulSoup helper functions
    @staticmethod
    def scrape_beautiful_text(page_source, beautiful_soup_parser='html.parser'):
        """takes page source and uses BeautifulSoup to extract a list of all visible text items on page"""

        # Couple of helper functions
        def tag_visible(element):
            """checks BeautifulSoup element to see if it is visible on webpage"""

            """original list of elements:
            ['style', 'script', 'head', 'title', 'meta', '[document]']
            """
            if element.parent.name in ['i:pgf', 'svg', 'img', 'script', 'style', 'script', 'head', 'title', 'meta', '[document]']:
                return False
            if isinstance(element, Comment):
                return False
            return True

        def text_from_html(soup):
            """take BeautifulSoup entity, finds all text blocks, and checks if block is visible on page"""
            texts = soup.findAll(text=True)
            visible_texts = filter(tag_visible, texts)
            return visible_texts

        def anyalpha(string):
            """Check for any alpha"""
            return any([c.isalpha() for c in string])

        # Create soup
        soup = BeautifulSoup(page_source, beautiful_soup_parser)

        # I may be able to simplify this... just if t?
        text = [t.strip() for t in text_from_html(soup) if t.strip()]
        # Only return is there is at least some alphabetic info
        text = [t for t in text if anyalpha(t)]
        # Add the page title as the first entry to the text
        if soup.title:
            title = soup.title.text.strip()
            return [title] + text
        else:
            return text

    @staticmethod
    def validate_urls_from_params(params_url_text, allowed_schemes=None):
        """
        Primarily designed to work with Search.validate_query() which expects a text string of urls. Users are (should
        be) told to separate by newlines, however, most other inputs are separated by commas. This function will take a
        string of URLs and return a validated list and list of invalid urls (which can then be used to inform the user).

        Note: some urls may contain scheme (e.g., https://web.archive.org/web/20250000000000*/http://economist.com);
        this function will work so long as the inner scheme does not follow a comma (e.g., "http://,https://"). Future
        problems.

        :param str params_url_text:  Text string of URLs separated by newlines or commas
        :param tuple allowed_schemes:  Tuple of allowed schemes (default: ('http://', 'https://', 'ftp://', 'ftps://'))
        """
        if allowed_schemes is None:
            allowed_schemes = ('http://', 'https://', 'ftp://', 'ftps://')
        potential_urls = []
        # Split the text by \n
        for line in params_url_text.split('\n'):
            # Handle commas that may exist within URLs
            parts = line.split(',')
            recombined_url = ""
            for part in parts:
                if part.startswith(allowed_schemes):  # Other schemes exist
                    # New URL start detected
                    if recombined_url:
                        # Already have a URL, add to list
                        potential_urls.append(recombined_url)
                    # Start new URL
                    recombined_url = part
                elif part:
                    if recombined_url:
                        # Add to existing URL
                        recombined_url += "," + part
                    else:
                        # No existing URL, start new
                        recombined_url = part
                else:
                    # Ignore empty strings
                    pass
            if recombined_url:
                # Add any remaining URL
                potential_urls.append(recombined_url.strip()) # Remove any trailing whitespace

        validated_urls = []
        invalid_urls = []
        for url in potential_urls:
            # requote_uri will fix any issues with spaces and other characters; seems better than urllib.parse.quote which does not work if the url is already quoted
            url = requote_uri(url)
            if is_url(url, require_protocol=True):
                validated_urls.append(url)
            else:
                invalid_urls.append(url)

        return validated_urls, invalid_urls

    @staticmethod
    def get_beautiful_links(page_source, domain, beautiful_soup_parser='html.parser'):
        """
        takes page_source and creates BeautifulSoup entity and url that was scraped, finds all links,
        and returns the number of links and a list of all links in tuple of shown text, fixed link,
        and original link.

        Uses domain to attempt to fix links that are partial.
        """
        soup = BeautifulSoup(page_source, beautiful_soup_parser)
        url_count = 0
        all_links= soup.findAll('a')
        links_to_return = []
        for link in all_links:
            link_url = link.get('href')
            original_url = link_url
            link_text = None
            if link_url is not None:
                url_count += 1
                link_text = link.text
                # If image in link, find alt text and add to link_text
                for img in link.findAll('img'):
                    alt_text = img.get('alt')
                    if alt_text and type(alt_text) == str:
                        link_text = ' '.join([link_text, alt_text])
                # Fix URL if needed
                if link_url.strip()[:4] == "http":
                    pass
                else:
                    link_url = urljoin(domain, link_url)
            else:
                continue
            links_to_return.append({'link_text': link_text,
                                    'url': link_url.rstrip('/'),
                                    'original_url': original_url})
        return url_count, links_to_return

    @staticmethod
    def get_beautiful_iframe_links(page_source, beautiful_soup_parser='html.parser'):
        """
        takes page_source and creates BeautifulSoup entity, then looks for iframes
        and gets their src link. This could perhaps be more robust. Selenium can
        also switch to iframes to extract html/text, but you have to know a bit
        more in order to select them (xpath, css, etc.).

        You could then either use requests of selenium to scrape these links.
        TODO: is it possible/desirable to insert the html source code back into
        the original url?
        """
        iframe_links = []
        soup = BeautifulSoup(page_source, beautiful_soup_parser)
        iframes = soup.findAll('iframe')
        if iframes:
            for iframe in iframes:
                if iframe.get('src'):
                    iframe_links.append(iframe.get('src'))
                elif iframe.get('data-src'):
                    iframe_links.append(iframe.get('data-src'))
                elif iframe.get('data-url'):
                    # If no src, then it is likely a data-url
                    iframe_links.append('data-url')
                else:
                    # unknown iframe
                    # TODO: add logging? in this staticmethod...
                    pass
        return iframe_links

    def scroll_down_page_to_load(self, max_time=None):
        """
        Scroll down page until it is fully loaded. Returns top of window at end.
        """

        def _scroll_to_top():
            try:
                self.driver.execute_script("window.scrollTo(0, 0);")
            except JavascriptException as e:
                # Apparently no window.scrollTo?
                action = ActionChains(self.driver)
                action.send_keys(Keys.HOME)
                action.perform()

        start_time = time.time()
        last_bottom = self.driver.execute_script('return window.scrollY')
        action = None
        while True:
            if max_time is not None:
                if time.time() - start_time > max_time:
                    # Stop if max_time exceeded
                    _scroll_to_top()
                    return last_bottom

            # Scroll down
            try:
                self.driver.execute_script("window.scrollTo(0, window.scrollY + window.innerHeight);")
            except JavascriptException as e:
                # Apparently no window.scrollTo?
                action = ActionChains(self.driver)
                action.send_keys(Keys.PAGE_DOWN)
                action.perform()

            # Wait for anything to load
            try:
                WebDriverWait(self.driver, max_time if max_time else None).until(
                    lambda driver: driver.execute_script('return document.readyState') == 'complete')
            except TimeoutException:
                # Stop if timeout
                _scroll_to_top()
                return last_bottom

            current_bottom = self.driver.execute_script('return window.scrollY')
            if last_bottom == current_bottom:
                # We've reached the bottom of the page
                _scroll_to_top()
                return current_bottom

            last_bottom = current_bottom
            time.sleep(.2)

    def kill_browser(self, browser):
        self.selenium_log.info(f"4CAT is killing {browser} with PID: {self.driver.service.process.pid}")
        try:
            subprocess.check_call(['kill', str(self.driver.service.process.pid)])
        except subprocess.CalledProcessError as e:
            self.selenium_log.error(f"Error killing {browser}: {e}")
            self.quit_selenium()
            raise e

    def destroy_to_click(self, button, max_time=5):
        """
        A most destructive way to click a button. If something is obscuring the button, it will be removed. Repeats
        destruction until the button is clicked or max_time is exceeded.

        Probably a good idea to reload after use if additional elements are needed

        :param button:  The button to click
        :param max_time:  Maximum time to attempt to click button
        """
        start_time = time.time()
        scrolled = False
        while True:
            try:
                button.click()
                self.selenium_log.debug("button clicked!")
                return True
            except ElementClickInterceptedException as e:
                if time.time() - start_time > max_time:
                    return False
                error = e
                self.selenium_log.debug(f"destroy_to_click: {error.msg}")

                error_element_type = error.msg.split("element <")[1].split(" ")[0].rstrip(">")
                if len(error.msg.split("element <")[1].split("class=\"")) > 1:
                    error_element_class = error.msg.split("element <")[1].split("class=\"")[1].split(" ")[0].rstrip("\">")
                else:
                    error_element_class = ""
                self.selenium_log.info(f"destroy_to_click removing element: ({error_element_type}{',' + error_element_class if error_element_class else ''})")

                self.driver.execute_script(
                    f"document.querySelector('{error_element_type}{'.' + error_element_class if error_element_class else ''}').remove();")
            except ElementNotInteractableException:
                if time.time() - start_time > max_time:
                    return False
                if not scrolled:
                    scrolled = True
                    # Try to scroll the button into view
                    try:
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                        time.sleep(1)
                    except JavascriptException as e:
                        self.selenium_log.debug(f"JavascriptException while scrolling into view: {e}")
                else:
                    self.selenium_log.debug("ElementNotInteractableException: consecutive unable to scroll into view and click")
                    return False
               

    def smart_click(self, button, max_time=10, strategies=['direct', 'scroll', 'wait', 'javascript', 'actions', 'destroy']):
        """
        Intelligently attempt to click a button using multiple strategies
        
        :param button: The button element to click
        :param max_time: Maximum time to spend attempting
        :param strategies: List of strategies to try in order
        """
        start_time = time.time()
        
        for strategy in strategies:
            if time.time() - start_time > max_time:
                self.selenium_log.warning(f"smart_click timeout after {max_time}s")
                return False
                
            try:
                if strategy == 'direct':
                    button.click()
                    return True
                    
                elif strategy == 'scroll':
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                    time.sleep(1)
                    button.click()
                    return True
                    
                elif strategy == 'wait':
                    WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable(button))
                    button.click()
                    return True
                    
                elif strategy == 'javascript':
                    self.driver.execute_script("arguments[0].click();", button)
                    return True
                    
                elif strategy == 'actions':
                    ActionChains(self.driver).move_to_element(button).click().perform()
                    return True
                    
                elif strategy == 'destroy':
                    return self.destroy_to_click(button, max_time - (time.time() - start_time))
                    
            except Exception as e:
                self.selenium_log.debug(f"Strategy '{strategy}' failed: {e}")
                continue
                
        return False            

    @staticmethod
    def is_selenium_available(config):
        """
        Checks for browser and webdriver
        """
        if not shutil.which(config.get("selenium.selenium_executable_path")):
            return False
        if not shutil.which(config.get("selenium.browser")):
            return False

        return True


class SeleniumSearch(SeleniumWrapper, Search, metaclass=abc.ABCMeta):
    """
    Selenium Scraper class

    Selenium utilizes a chrome webdriver and chrome browser to navigate and scrape the web. This processor can be used
    to initialize that browser and navigate it as needed. It replaces search to allow you to utilize the Selenium driver
    and ensure the webdriver and browser are properly closed out upon completion.
    """
    max_workers = 1
    config = {
        "selenium.browser": {
            "type": UserInput.OPTION_TEXT,
            "default": "firefox",
            "help": "Browser type ('firefox' or 'chrome')",
            "tooltip": "This must correspond to the installed webdriver; the fourcat_install.py script installs firefox and geckodriver",
        },
        "selenium.max_sites": {
            "type": UserInput.OPTION_TEXT,
            "default": 120,
            "help": "Posts per page",
            "coerce_type": int,
            "tooltip": "Posts to display per page"
        },
        "selenium.selenium_executable_path": {
            "type": UserInput.OPTION_TEXT,
            "default": "/usr/local/bin/geckodriver",
            "help": "Path to webdriver (geckodriver or chromedriver)",
            "tooltip": "fourcat_install.py installs to /usr/local/bin/geckodriver",
        },
        "selenium.firefox_extensions": {
            "type": UserInput.OPTION_TEXT_JSON,
            "default": {
                "i_dont_care_about_cookies": {"path": "", "always_enabled": False},
                },
            "help": "Firefox Extensions",
            "tooltip": "Can be used by certain processors and datasources",
        },
        "selenium.display_advanced_options": {
            "type": UserInput.OPTION_TOGGLE,
            "default": True,
            "help": "Show advanced options",
            "tooltip": "Show advanced options for Selenium processors",
        },
    }

    @classmethod
    def check_worker_available(cls, manager, modules):
        """
        Check if the worker can run. Here we check if there are too many
        workers of this type running already.

        :return bool:  True if the worker can run, False if not
        """
        # check if we have too many workers of this type running
        selenium_workers = 0
        for worker_type, workers in manager.worker_pool.items():
            worker_class = modules.workers[worker_type]
            if issubclass(worker_class, SeleniumSearch):
                selenium_workers += len(workers)

        if selenium_workers < cls.max_workers:
            return True
        else:
            return False

    def search(self, query):
        """
        Search for items matching the given query

        The real work is done by the get_items() method of the descending
        class. This method just provides some scaffolding and post-processing
        of results via `after_search()`, if it is defined.

        :param dict query:  Query parameters
        :return:  Iterable of matching items, or None if there are no results.
        """
        import time
        start = time.time()
        self.dataset.log(f"Checking for selenium {time.time() - start:.2f} seconds")
        if not self.is_selenium_available(config=self.config):
            raise ProcessorException("Selenium not available; please ensure browser and webdriver are installed and configured in settings")
        
        
        self.dataset.log(f"Starting selenium {time.time() - start:.2f} seconds")
        try:
            self.start_selenium(eager=self.eager_selenium)
        except ProcessorException as e:
            self.quit_selenium()
            raise e
        self.dataset.log(f"Started selenium {time.time() - start:.2f} seconds")
        # Returns to default position; i.e., 'data:,'
        try:
            self.reset_current_page()
        except InvalidSessionIdException as e:
            # Webdriver unable to connect to browser
            self.log.error(f"InvalidSessionIdException: {e}")
            self.quit_selenium()
            raise ProcessorException("Selenium or browser unable to start; please wait and try again later")

        # Sets timeout to 60; can be updated later if desired
        self.set_page_load_timeout()

        self.dataset.log(f"Collecting posts {time.time() - start:.2f} seconds")
        # Normal Search function to be used To be implemented by descending classes!
        try:
            posts = self.get_items(query)
        except Exception as e:
            # Ensure Selenium always quits
            self.quit_selenium()
            raise e

        if not posts:
            return None

        # search workers may define an 'after_search' hook that is called after
        # the query is first completed
        if hasattr(self, "after_search") and callable(self.after_search):
            posts = self.after_search(posts)

        return posts


    def clean_up(self):
        """
        Ensures Selenium webdriver and Chrome browser and closed whether processor completes successfully or not.
        """
        super().clean_up()

        self.quit_selenium()
