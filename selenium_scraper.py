import random
import subprocess
import time
import shutil
import signal
import abc
import os
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from bs4.element import Comment
from ural import is_url
from requests.utils import requote_uri
from urllib.parse import urlparse, parse_qs, unquote

from backend.lib.search import Search
from common.lib.exceptions import ProcessorException
from common.lib.user_input import UserInput

from selenium import webdriver
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options as FirefoxOptions
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
    browser_pid = None

    consecutive_errors = 0
    num_consecutive_errors_before_restart = 3

    _temp_profile_path = None
    _temp_profile_is_temp = False

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
        # ensure we only add a file handler once (avoid duplicate log entries)
        log_path = str(self.config.get("PATH_LOGS").joinpath('selenium.log'))
        existing = False
        for h in list(self.selenium_log.handlers):
            try:
                if isinstance(h, logging.FileHandler) and os.path.abspath(getattr(h, 'baseFilename', '')) == os.path.abspath(log_path):
                    # update formatter in case it's changed and mark as present
                    h.setFormatter(formatter)
                    existing = True
                    break
            except Exception:
                continue
        if not existing:
            file_handler = logging.FileHandler(log_path)
            file_handler.setFormatter(formatter)
            self.selenium_log.addHandler(file_handler)
        # Avoid propagating to ancestor loggers (prevents duplicate writes if root logger also has handlers)
        self.selenium_log.propagate = False

        self._setup_done = True

    def get_firefox_neterror_info(self):
        """
        Returns (is_neterror, reason, target_url, raw_url) based on Firefox about:neterror.
        """
        try:
            current = self.driver.current_url
        except UnexpectedAlertPresentException:
            self.dismiss_alert()
            current = self.driver.current_url

        if isinstance(current, str) and current.startswith("about:neterror"):
            try:
                qs = parse_qs(urlparse(current).query)
                reason = (qs.get("e", [""])[0] or "").strip()
                target = unquote((qs.get("u", [""])[0] or "").strip())
                return True, reason, target, current
            except Exception:
                return True, "", "", current
        return False, "", "", current

    @staticmethod
    def add_cookies(driver, cookies, url=None):
        """
        Add a cookie or list of cookies to a Selenium `driver`.

        Tries three strategies per cookie to maximise compatibility across driver versions:
          1. Full fidelity  — cookie dict as-is (expiry coerced to int).
          2. W3C pruned     — drop keys not in the W3C WebDriver spec (handles drivers that
                              reject unknown fields); sameSite is included here since modern
                              geckodriver supports it.
          3. Minimal safe   — name/value/domain/path/secure/httpOnly/expiry only (broadest
                              compatibility with older drivers).

        cookies: dict or list of dicts. Each cookie must include 'name' and 'value'.
                 Optional keys: 'domain', 'path', 'expiry' (unix seconds), 'secure',
                 'httpOnly', 'sameSite'.
        url: optional full URL (scheme+host) to navigate to first — required if cookie
             domain must match current page origin.

        Returns a list of (cookie_name, exception) tuples for cookies that could not be
        added under any strategy.
        """
        if not isinstance(cookies, (list, tuple)):
            cookies = [cookies]
        if url:
            driver.get(url)

        # Keys accepted by modern W3C WebDriver (geckodriver ≥ 0.30, chromedriver ≥ 100)
        _w3c_keys = {'name', 'value', 'path', 'domain', 'secure', 'httpOnly', 'expiry', 'sameSite'}
        # Minimal set for older/stricter driver versions
        _minimal_keys = {'name', 'value', 'path', 'domain', 'secure', 'httpOnly', 'expiry'}

        failures = []
        for c in cookies:
            cookie = c.copy()
            # Coerce expiry to int; remove it if not convertible
            if 'expiry' in cookie and cookie['expiry'] is not None:
                try:
                    cookie['expiry'] = int(cookie['expiry'])
                except (TypeError, ValueError):
                    del cookie['expiry']
            name = cookie.get('name', '<unknown>')

            # Strategy 1: full cookie dict as-is
            try:
                driver.add_cookie(cookie)
                continue
            except Exception:
                pass

            # Strategy 2: W3C keys (drops any custom/unknown fields the driver rejects)
            try:
                pruned_w3c = {k: v for k, v in cookie.items() if k in _w3c_keys and v is not None}
                driver.add_cookie(pruned_w3c)
                continue
            except Exception:
                pass

            # Strategy 3: minimal safe set
            try:
                pruned_min = {k: v for k, v in cookie.items() if k in _minimal_keys and v is not None}
                driver.add_cookie(pruned_min)
                continue
            except Exception as e_min:
                failures.append((name, e_min))

        return failures

    def _normalize_domain(self, domain: str):
        """Normalize a cookie domain (strip leading dot and lower-case)."""
        if not domain:
            return None
        return domain.lstrip('.').lower()

    def _domain_matches(self, host: str, cookie_domain: str):
        """Return True if `cookie_domain` applies to `host` (handles subdomains)."""
        if not host or not cookie_domain:
            return False
        host = host.lower()
        cd = self._normalize_domain(cookie_domain)
        if host == cd:
            return True
        # match subdomains
        return host.endswith('.' + cd)

    def _group_cookies_by_domain(self, cookie_jar, default_host=None):
        """Group a list of cookie dicts by normalized domain.

        Cookies without a `domain` key are assigned to `default_host`.
        Returns a dict: {normalized_domain: [cookie_dict, ...]}
        """
        grouped = {}
        if not cookie_jar:
            return grouped
        for c in cookie_jar:
            domain = c.get('domain')
            if domain:
                nd = self._normalize_domain(domain)
            else:
                nd = default_host.lower() if default_host else None
            if not nd:
                continue
            grouped.setdefault(nd, []).append(c)
        return grouped

    def apply_cookies_for_url(self, url, cookie_jar):
        """Apply cookies appropriate for `url` from `cookie_jar`.

        Returns a list of errors encountered (empty if none).
        """
        errors = []
        if not cookie_jar:
            return [Exception("No cookies provided in cookie_jar")]
        if not self.driver:
            return [Exception("Selenium driver not initialized")]

        try:
            host = urlparse(url).hostname
        except Exception:
            host = None
        if not host:
            return [Exception("Invalid URL or unable to extract hostname")]

        grouped = self._group_cookies_by_domain(cookie_jar, default_host=host)
        # Ensure cache exists
        if not hasattr(self, '_cookie_domains_applied') or self._cookie_domains_applied is None:
            self._cookie_domains_applied = set()

        for domain, cookies in grouped.items():
            # Only apply cookies that match the target host
            if not self._domain_matches(host, domain) and domain != host:
                continue

            if domain in self._cookie_domains_applied:
                # already applied for this session
                continue

            # choose scheme: prefer https if any cookie is secure
            scheme = 'https' if any(c.get('secure') for c in cookies) else 'http'
            origin = f"{scheme}://{domain}"

            # Navigate to domain once to set context for cookies; required for Selenium to accept them
            self.driver.get(origin)  

            try:
                # Add cookies; add_cookies() returns (name, exc) pairs for any that failed
                add_failures = type(self).add_cookies(self.driver, cookies)
                self._cookie_domains_applied.add(domain)

                # Log per-cookie failures (all three strategies exhausted)
                for cookie_name, exc in (add_failures or []):
                    msg = f"Failed to add cookie '{cookie_name}' for {domain} (all strategies): {type(exc).__name__}: {repr(exc)}"
                    if self.selenium_log:
                        self.selenium_log.warning(msg)
                    errors.append(exc)

                # Verify which cookies landed and log discrepancies to aid debugging
                if hasattr(self, 'selenium_log') and self.selenium_log and self.selenium_log.isEnabledFor(logging.DEBUG):
                    try:
                        present = {c['name']: c for c in self.driver.get_cookies()}
                        for ck in cookies:
                            cname = ck.get('name')
                            if not cname:
                                continue
                            cur = present.get(cname)
                            if not cur:
                                self.selenium_log.warning(f"Cookie '{cname}' not present in browser after add (domain={domain})")
                            else:
                                diffs = []
                                for key in ('value', 'domain', 'path', 'secure', 'httpOnly', 'expiry', 'sameSite'):
                                    req_val = ck.get(key)
                                    got_val = cur.get(key)
                                    if req_val is None and got_val is None:
                                        continue
                                    if str(req_val) != str(got_val):
                                        diffs.append((key, req_val, got_val))
                                if diffs:
                                    self.selenium_log.debug(f"Cookie '{cname}' stored with diffs vs requested: {diffs}")
                    except Exception as e:
                        self.selenium_log.warning(f"Error verifying cookies for {domain}: {type(e).__name__}: {repr(e)}")

            except Exception as e:
                # Catch unexpected errors from the overall block (navigation, etc.)
                try:
                    if hasattr(self, 'selenium_log') and self.selenium_log:
                        self.selenium_log.warning(f"Error applying cookies for {domain}: {type(e).__name__}: {repr(e)}")
                except Exception:
                    pass
                errors.append(e)

        return errors

    def get_with_error_handling(self, url, max_attempts=1, wait=0, cookie_jar=None, restart_browser=True):
        """
        Attempts to call driver.get(url) with error handling. Will attempt to restart Selenium if it fails and can
        attempt to kill Firefox (and allow Selenium to restart) itself if allowed.

        Returns a tuple containing a bool (True if successful, False if not) and a list of the errors raised.

        :param str url:                URL to retrieve
        :param int max_attempts:       Maximum number of attempts to retrieve the URL
        :param int wait:               Seconds to wait between attempts
        :param list cookie_jar:        List of cookies to add to the driver
        :param bool restart_browser:   If True, will kill the browser process if too many consecutive errors occur
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

        # Apply any user-provided cookies appropriate for this URL
        if cookie_jar:
            try:
                cookie_errors = self.apply_cookies_for_url(url, cookie_jar)
                if cookie_errors:
                    # convert exceptions/messages into errors list for caller visibility
                    [errors.append(e) for e in cookie_errors]
            except Exception as e:
                errors.append(e)
        while attempts < max_attempts:
            attempts += 1
            try:
                # Wrap navigation to auto-handle sporadic, unexpected alerts without JS overrides
                self.safe_action(lambda: self.driver.get(url))
                # Detect Firefox neterror and treat as failure
                is_ne, reason, target, raw = self.get_firefox_neterror_info()
                if is_ne:
                    msg = f"Firefox neterror '{reason or 'unknown'}' loading {target or url}"
                    self.selenium_log.warning(msg)
                    errors.append(msg)
                    success = False
                    self.consecutive_errors += 1
                else:
                    success = True
                    self.consecutive_errors = 0
            except TimeoutException as e:
                errors.append(f"Timeout retrieving {url}: {e}")
            except Exception as e:
                self.selenium_log.error(f"Error driver.get({url}){(' (dataset '+self.dataset.key+') ') if hasattr(self, 'dataset') else ''}: {e}")
                errors.append(e)
                self.consecutive_errors += 1

            # Restart after too many consecutive failures
            if self.consecutive_errors > self.num_consecutive_errors_before_restart:
                self.restart_selenium(kill_browser=restart_browser)

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
        # self.selenium_log.debug(f"Current cookies: {self.driver.get_cookies() if self.driver else 'N/A'}")
        return success, errors

    def simple_scrape_page(self, url, extract_links=False, title_404_strings='default', wait=0, max_attempts=1, user_cookies=None):
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

        get_success, errors = self.get_with_error_handling(url, cookie_jar=user_cookies, max_attempts=max_attempts, wait=wait, restart_browser=True)
        if get_success:
            result = self.collect_results(url, extract_links, title_404_strings)
            if errors:
                result['errors'].extend(errors)
            return result
        else:
            if errors:
                return {'errors': errors}
            return False


    def collect_results(self, url, extract_links=False, title_404_strings='default'):
        """
        Collect results from the current page. Returns a dictionary containing basic results from scrape including final_url,
        page_title, and page_source. Optionally can include links if extract_links is True. Handles errors from driver.title, driver.current_url, and driver.page_source gracefully by logging the error and including it in the returned dictionary under an 'errors' key as a list of error messages. Note that if an error occurs when trying to access any of these properties, the corresponding value in the returned dictionary will be an empty string.

        :param str url:  url as string; beginning with scheme (e.g., http, https)
        :param bool extract_links:  Whether to extract links from the page
        :param List title_404_strings:  List of strings representing possible 404 text to be compared with driver.title
        :return dict: A dictionary containing basic results from scrape including final_url, page_title, and page_source.
        """
        errors = []
        try:
            detected_404 = self.check_for_404(title_404_strings)
        except Exception as e:
            self.selenium_log.warning(f"Error checking for 404: {e}")
            errors.append(e)
            detected_404 = False
        try:
            title = self.driver.title
        except Exception as e:
            self.selenium_log.warning(f"Error getting page title: {e}")
            errors.append(e)
            title = ""
        try:
            final_url = self.driver.current_url
        except Exception as e:
            self.selenium_log.warning(f"Error getting final URL: {e}")
            errors.append(e)
            final_url = ""
        try:
            page_source = self.driver.page_source
        except Exception as e:
            self.selenium_log.warning(f"Error getting page source: {e}")
            errors.append(e)
            page_source = ""

        result = {
            'original_url': url,
            'detected_404': detected_404,
            'page_title': title,
            'final_url': final_url,
            'page_source': page_source,
            'errors': errors
            }

        if extract_links:
            try:
                result['links'] = self.collect_links()
            except Exception as e:
                self.selenium_log.warning(f"Error collecting links: {e}")
                result['errors'].append(e)

        result["success"] = True if final_url and page_source and not detected_404 else False

        return result

    def collect_links(self):
        """
        Collect all links on the current page. Returns a list of URLs (strings).
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

    def start_selenium(self, browser=None, eager=None, proxy=None, config=None):
        """
        Start a browser with Selenium

        :param bool eager:  Eager loading? If None, uses class attribute self.eager_selenium (default False)
        """        
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
        
        self.proxy = proxy

        if eager is not None:
            # Update eager loading
            self.eager_selenium = eager

        if browser is not None:
            # Update browser type
            self.browser = browser
        elif self.browser is None:
            # Use configured default browser
            self.browser = self.config.get('selenium.browser')
        # Track which domains we've already applied cookies for in this session
        self._cookie_domains_applied = set()
        self.selenium_log.info(f"Starting Selenium with browser: {self.browser}")
        
        if self.browser != "firefox":
            raise NotImplementedError("Currently only Firefox is supported")
        else:
            self.setup_firefox()
        
        self.last_scraped_url = None
        self.browser_pid = self.driver.service.process.pid

    def setup_firefox(self):
        """
        Setup Firefox-specific options for Selenium.
        """
        driver_start = time.time()
        options = FirefoxOptions()

        # Configure virtual display vs headless mode
        self.setup_virtual_display_mode(options, "firefox")
       
        # Resolve profile first so we can decide whether to start in private mode.
        # A user-configured path takes priority; if none is set, create a fresh temp profile
        # for this session so each job gets a clean, isolated browser context.
        profile_path = None
        try:
            profile_path = self.get_profile()
            if not profile_path:
                profile_path = self._create_temp_profile()
        except Exception as e:
            self.selenium_log.warning(f"Could not resolve Firefox profile: {e}")

        # Firefox-specific options
        options.add_argument('--no-sandbox')
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")

        # Base preferences
        options.set_preference("dom.webdriver.enabled", False)
        options.set_preference('useAutomationExtension', False)
        options.set_preference("browser.cache.disk.enable", False)
        options.set_preference("browser.cache.memory.enable", False)

        if not profile_path:
            # No profile available — use private mode for basic session isolation
            options.add_argument("--private")
            options.set_preference("browser.privatebrowsing.autostart", True)
        # Optionally adjust prefs that reduce disruptive dialogs; kept behind config for stealth
        if self.config.get('selenium.reduce_dialog_prefs', False):
            options.set_preference("dom.webnotifications.enabled", False)
            options.set_preference("dom.push.enabled", False)
            # Disabling beforeunload prevents sites from prompting on leave; toggle off if detection suspected
            options.set_preference("dom.disable_beforeunload", True)
        # Configure unhandled prompt behavior (internal to webdriver; page JS cannot read)
        try:
            behavior = self.config.get('selenium.unhandled_prompt_behavior', 'dismiss') if self.config else 'dismiss'
            # W3C capability name
            options.set_capability('unhandledPromptBehavior', behavior)
        except Exception:
            pass

        # TODO: setting to block images; REMOVE for screenshot capture
        # options.set_preference("permissions.default.image", 2)  # Block images for speed

        # Eager loading
        if self.eager_selenium:
            options.set_capability("pageLoadStrategy", "eager")
        
        # Set custom user agent
        user_agent = self.get_user_agent()
        options.set_preference("general.useragent.override", user_agent)

        # Configure proxy if provided
        if self.proxy is not None:
            # Parse proxy string (expected format: "protocol://host:port" or "host:port")
            if "://" in self.proxy:
                proxy_parts = self.proxy.split("://")
                proxy_type = proxy_parts[0].lower()
                proxy_host_port = proxy_parts[1]
            else:
                proxy_type = "http"  # Default to HTTP proxy
                proxy_host_port = self.proxy
            
            if ":" in proxy_host_port:
                proxy_host, proxy_port = proxy_host_port.split(":")
                proxy_port = int(proxy_port)
            else:
                proxy_host = proxy_host_port
                proxy_port = 8080  # Default port
            
            # Set proxy preferences
            if proxy_type in ["http", "https"]:
                options.set_preference("network.proxy.type", 1)  # Manual proxy configuration
                options.set_preference("network.proxy.http", proxy_host)
                options.set_preference("network.proxy.http_port", proxy_port)
                options.set_preference("network.proxy.ssl", proxy_host)
                options.set_preference("network.proxy.ssl_port", proxy_port)
            elif proxy_type == "socks":
                options.set_preference("network.proxy.type", 1)
                options.set_preference("network.proxy.socks", proxy_host)
                options.set_preference("network.proxy.socks_port", proxy_port)
                options.set_preference("network.proxy.socks_version", 5)  # SOCKS5
            
            # Don't use proxy for localhost
            options.set_preference("network.proxy.no_proxies_on", "localhost, 127.0.0.1")

        # Set Firefox binary path if configured
        firefox_binary = self.config.get('selenium.firefox_binary_path', None)
        if firefox_binary and os.path.exists(firefox_binary):
            options.binary_location = firefox_binary
            self.selenium_log.info(f"Using custom Firefox binary: {firefox_binary}")

        # Apply the resolved profile (user-provided or temp)
        if profile_path:
            options.add_argument(f'--profile={profile_path}')
            self.selenium_log.info(f"Using Firefox profile: {profile_path}")

        try:
            # Create Firefox service with configurable geckodriver path
            service_kwargs = {"log_output": str(self.config.get("PATH_LOGS").joinpath('geckodriver.log'))}
            geckodriver_path = self.config.get('selenium.selenium_executable_path', '/usr/local/bin/geckodriver')
            if geckodriver_path and os.path.exists(geckodriver_path):
                service_kwargs['executable_path'] = geckodriver_path
                self.selenium_log.debug(f"Using custom geckodriver: {geckodriver_path}")
            
            self.selenium_log.debug("Selenium env: DISPLAY=%s, LIBGL_ALWAYS_SOFTWARE=%s", os.environ.get('DISPLAY'), os.environ.get('LIBGL_ALWAYS_SOFTWARE'))
            self.selenium_log.debug("Will start geckodriver with kwargs: %s", service_kwargs)
            service = FirefoxService(**service_kwargs)
            
            # Create Firefox driver
            self.driver = webdriver.Firefox(service=service, options=options)
            
            # Apply common configuration
            self.apply_common_driver_config()
            
        except (SessionNotCreatedException, WebDriverException) as e:
            self.selenium_log.error(f"Error starting Firefox driver: {e}")
            raise ProcessorException("Could not connect to browser (%s)." % str(e))
        except Exception as e:
            self.selenium_log.error(f"Unexpected error starting Firefox driver: {e}")
            try:
                gl = str(self.config.get("PATH_LOGS").joinpath('geckodriver.log'))
                if os.path.exists(gl):
                    with open(gl,'rb') as f:
                        f.seek(0,2)
                        size = f.tell()
                        f.seek(max(0, size-20000))
                        tail = f.read().decode('utf-8', errors='replace')
                    self.selenium_log.error("Geckodriver tail:\n%s", tail)
            except Exception:
                pass
            raise ProcessorException("Unexpected error starting browser: %s" % str(e))
                
        driver_time = time.time() - driver_start        
        self.selenium_log.info(f"Firefox driver creation took: {driver_time:.2f}s (PID: {self.driver.service.process.pid})")

    def setup_virtual_display_mode(self, options, browser_type="generic"):
        """
        Configure virtual display vs headless mode for any browser
        
        :param options: Browser options object (ChromeOptions, FirefoxOptions, etc.)
        :param browser_type: Type of browser for logging ("firefox", "chrome", "undetected-chrome")
        :return: bool indicating if virtual display is being used
        """
        use_virtual_display = self.config.get('selenium.use_virtual_display', False)
        display_available = self.start_virtual_display() if use_virtual_display else False

        if display_available:
            self.selenium_log.debug(f"Using virtual display for {browser_type} (better anti-detection)")
            return True
        else:
            self.selenium_log.warning(f"Using headless mode for {browser_type} (virtual display not available or disabled)")
            # Set headless mode - different for different browsers
            if hasattr(options, 'headless'):
                options.headless = True
            if hasattr(options, 'add_argument'):
                options.add_argument('--headless')
            return False

    def start_virtual_display(self):
        """
        Start virtual display using Xvfb for anti-detection
        This makes browsers think they're running in a real display environment

        :return: 
        """
        if not hasattr(self, 'xvfb_process') or self.xvfb_process is None:
            try:
                import subprocess
                import os
                
                # Check if DISPLAY environment variable is set
                display = os.environ.get('DISPLAY', ':99')
                
                # Check if Xvfb is already running on this display
                try:
                    result = subprocess.run(['ps', 'aux'], capture_output=True, text=True, timeout=5)
                    if f'Xvfb {display}' in result.stdout:
                        self.selenium_log.debug(f"Xvfb already running on display {display}")
                        # Ensure DISPLAY is exported even if we didn't start Xvfb ourselves
                        os.environ['DISPLAY'] = display
                        self.xvfb_process = None  # We didn't start it, so don't try to stop it
                        return True
                except Exception:
                    pass
                
                # Start Xvfb
                width = os.environ.get('SCREEN_WIDTH', '1920')
                height = os.environ.get('SCREEN_HEIGHT', '1080')
                depth = os.environ.get('SCREEN_DEPTH', '24')
                
                xvfb_cmd = [
                    'Xvfb', display,
                    '-screen', '0', f'{width}x{height}x{depth}',
                    '-ac', '+extension', 'GLX', '+render', '-noreset'
                ]
                
                self.xvfb_process = subprocess.Popen(
                    xvfb_cmd,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    preexec_fn=os.setsid if hasattr(os, 'setsid') else None
                )
                
                # Wait a moment for Xvfb to start
                time.sleep(1)
                
                # Set DISPLAY environment variable for this process
                os.environ['DISPLAY'] = display
                
                self.selenium_log.debug(f"Started Xvfb on display {display} with resolution {width}x{height}x{depth}")
                return True
                
            except Exception as e:
                self.selenium_log.warning(f"Failed to start virtual display: {e}. Falling back to headless mode.")
                # Fall back to headless mode if Xvfb fails
                self.xvfb_process = None
                return False

    def stop_virtual_display(self):
        """
        Stop virtual display if we started it
        """
        if hasattr(self, 'xvfb_process') and self.xvfb_process is not None:
            try:
                self.xvfb_process.terminate()
                self.xvfb_process.wait(timeout=5)
                self.selenium_log.debug("Stopped virtual display")
            except Exception as e:
                self.selenium_log.warning(f"Error stopping virtual display: {e}")
                try:
                    self.xvfb_process.kill()
                except Exception:
                    pass
            finally:
                self.xvfb_process = None

    def get_user_agent(self):
        """
        Get user agent for the browser
        """
        # TODO: add input for agents to frontend/config
        # Check out https://github.com/fake-useragent/fake-useragent
        # ua = UserAgent(platforms='desktop', os=["Mac OS X", "Windows"], browsers="Firefox") for Firefox for example
        if self.browser == "firefox":
            agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0",
            ]
        else:
            raise NotImplementedError(f"User agent retrieval not implemented for browser type: {self.browser}")
        
        return random.choice(agents)

    def get_profile(self):
        """
        Get the user profile for the browser if it exists. This is used to allow Selenium to use an existing profile.

        Can be overridden by subclasses to provide a specific profile path.
        """
        try:
            # Determine active browser
            browser = self.browser or self.config.get('selenium.browser')

            path = None
            if browser == 'firefox':
                path = self.config.get('selenium.firefox_profile_path', None)
            else:
                raise NotImplementedError("Currently only Firefox is supported")

            if not path:
                return None

            # Normalize env and relative paths
            path = os.path.expanduser(os.path.expandvars(path))
            if not os.path.isabs(path):
                base = self.config.get('PATH_ROOT')
                path = os.path.abspath(os.path.join(base, path))

            if os.path.exists(path):
                return path
            else:
                if hasattr(self, 'log') and self.log:
                    self.log.warning(f"Configured profile not found at {path}; ignoring")
                self.selenium_log.warning(f"Configured profile not found at {path}; ignoring")
                return None
        except Exception as e:
            if hasattr(self, 'log') and self.log:
                self.log.warning(f"get_profile() error: {e}")
            self.selenium_log.warning(f"get_profile() error: {e}")
            return None
        
    def _create_temp_profile(self):
        """
        Create a fresh, isolated browser profile directory for this session.

        The path is derived from PATH_DATA, the dataset key (if available) and the browser
        type, so the directory name is predictable and easy to audit/clean up manually.
        The caller is responsible for passing the path to the browser (e.g. via --profile).

        Sets self._temp_profile_path and self._temp_profile_is_temp = True.

        :return str: Absolute path to the created profile directory.
        """
        # Build a human-readable, collision-resistant name
        dataset_key = getattr(self.dataset, 'key', None) if hasattr(self, 'dataset') else None
        browser = self.browser or 'firefox'
        if dataset_key:
            profile_name = f"{dataset_key}_{browser}_temp_profile"
        else:
            # Fallback: timestamp + pid so concurrent workers don't collide
            import datetime
            profile_name = f"{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}_{os.getpid()}_{browser}_temp_profile"

        try:
            base_dir = self.config.get('PATH_DATA')
            profile_path = str(base_dir.joinpath(profile_name))
        except Exception:
            # Last resort: system temp dir
            import tempfile
            profile_path = os.path.join(tempfile.gettempdir(), profile_name)

        os.makedirs(profile_path, exist_ok=True)
        self._temp_profile_path = profile_path
        self._temp_profile_is_temp = True
        self.selenium_log.info(f"Created temporary Firefox profile: {profile_path}")
        try:
            if hasattr(self, 'dataset') and self.dataset:
                self.dataset.log(f"Created temporary Firefox profile: {profile_path}")
        except Exception:
            pass
        return profile_path

    def _remove_temp_profile(self):
        """
        Remove the temporary profile directory created by _create_temp_profile(), if any.
        Only removes directories we created ourselves (self._temp_profile_is_temp == True).
        User-provided profile paths are never deleted.
        """
        if not self._temp_profile_is_temp or not self._temp_profile_path:
            return
        path = self._temp_profile_path
        self._temp_profile_path = None
        self._temp_profile_is_temp = False
        if not os.path.exists(path):
            return
        try:
            shutil.rmtree(path, ignore_errors=False)
            self.selenium_log.info(f"Removed temporary Firefox profile: {path}")
        except Exception as e:
            # On Windows, Firefox may briefly hold file locks after quit(); retry once
            self.selenium_log.warning(f"Could not remove temp profile on first attempt ({e}); retrying in 2s")
            time.sleep(2)
            shutil.rmtree(path, ignore_errors=True)
            if not os.path.exists(path):
                self.selenium_log.info(f"Removed temporary Firefox profile (retry): {path}")
            else:
                self.selenium_log.warning(f"Temp profile may still exist at {path}; manual cleanup may be needed")

    def apply_common_driver_config(self):
        """
        Apply common driver configuration after driver creation.
        """
        if not self.driver:
            return
            
        # Apply timeouts from config
        page_timeout = self.config.get('selenium.page_load_timeout', 60)
        implicit_wait = self.config.get('selenium.implicit_wait', 10)

        self.driver.set_page_load_timeout(page_timeout)
        self.driver.implicitly_wait(implicit_wait)
        
        # Set window size to common resolution; maximize if possible
        self.driver.set_window_size(1920, 1080)
        self.driver.maximize_window()
        
        # Remove webdriver detection
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        self.selenium_log.debug(f"Applied page load timeout: {page_timeout}s")
        self.selenium_log.debug(f"Applied implicit wait: {implicit_wait}s")

    def quit_selenium(self, kill_browser=False):
        """
        Always attempt to close the browser otherwise multiple versions of Chrome will be left running.

        And Chrome is a memory hungry monster.
        """        
        try:
            self.driver.quit()
        except Exception as e:
            self.selenium_log.error(e)
        self.driver = None
        self.last_scraped_url = None
        # Clear applied-cookie domain cache
        try:
            self._cookie_domains_applied = set()
        except Exception:
            pass

        if kill_browser:
            time.sleep(2)
            self.kill_browser()

        # Clear browser PID
        self.browser_pid = None

        # Stop virtual display (only if we started it)
        self.stop_virtual_display()

        # Remove temp profile if we created it for this session
        self._remove_temp_profile()

    def restart_selenium(self, eager=None, kill_browser=False):
        """
        Weird Selenium error? Restart and try again.
        """
        self.quit_selenium(kill_browser=kill_browser)
        self.start_selenium(eager=eager)
        self.reset_current_page()

    def set_page_load_timeout(self, timeout=60):
        """
        Adjust the time that Selenium will wait for a page to load before failing
        """
        self.driver.set_page_load_timeout(timeout)

    def check_for_movement(self, old_element=None, wait_time=5):
        """
        Some driver.get() commands will not result in an error even if they do not result in updating the page source.
        This can happen, for example, if a url directs the browser to attempt to download a file. It can therefore be
        important to check and ensure a new page was actually obtained before retrieving the page source as you will
        otherwise retrieve he same information from the previous url.

        WARNING: It may also be true that a url redirects to the same url as previous scraped url. This check would assume no
        movement occurred. Use in conjunction with self.reset_current_page() if it is necessary to check every url results
        and identify redirects.
        """
        if old_element:
            # If an old element is provided, wait for it to become stale
            try:
                WebDriverWait(self.driver, wait_time).until(EC.staleness_of(old_element))
            except (TimeoutException, ElementNotInteractableException, ElementClickInterceptedException) as e:
                # If the element is not stale, we assume no movement occurred
                self.selenium_log.warning(f"{e}: Element did not become stale; assuming no movement occurred.")
                return False
        
        # Check if the current URL is different from the last scraped URL
        try:
            current_url = self.driver.current_url
        except UnexpectedAlertPresentException:
            # attempt to dismiss random alert
            self.dismiss_alert()
            current_url = self.driver.current_url

        # Treat Firefox error page as no movement
        is_ne, reason, target, raw = self.get_firefox_neterror_info()
        if is_ne:
            self.selenium_log.debug(f"Treated about:neterror as no movement: {reason} for {target or current_url}")
            return False

        if current_url == self.last_scraped_url:
            return False
        else:
            return True

    def dismiss_alert(self):
        """Attempt to dismiss or accept any present alert with minimal side-effects.

        Stealth approach:
        - Do NOT monkey patch window.alert/confirm/prompt globally.
        - Small random reaction delay to mimic a human noticing the dialog.
        - Track origin counts (not currently used for escalation but available).
        Returns True if an alert was handled, else False.
        """
        if not self.driver:
            return False
        current_window_handle = self.driver.current_window_handle
        try:
            alert = self.driver.switch_to.alert
        except NoAlertPresentException:
            return False
        except UnexpectedAlertPresentException:
            # Retry once
            try:
                alert = self.driver.switch_to.alert
            except Exception:
                return False
        except Exception:
            return False

        # Brief delay (50-170 ms)
        time.sleep(random.uniform(0.05, 0.17))
        acted = 'none'
        try:
            alert.dismiss()
            acted = 'dismissed'
        except Exception:
            try:
                alert.accept()
                acted = 'accepted'
            except Exception:
                acted = 'unhandled'

        # Return to original window (alert switch may change focus)
        self.driver.switch_to.window(current_window_handle)

        if self.selenium_log:
            self.selenium_log.debug(f"Alert: {acted} - Text: {getattr(alert, 'text', '')}")
        return acted in ('dismissed', 'accepted')

    def safe_action(self, callable_obj, retries=2, delay=0.25):
        """Execute a WebDriver action handling sporadic UnexpectedAlertPresentException.

        Strategy:
        - Attempt callable.
        - If UnexpectedAlertPresentException occurs, dismiss alert, jittered delay, retry.
        - Avoid global JS overrides to remain less detectable.

        :param callable_obj: Zero-argument function wrapping WebDriver call.
        :param retries: Times to retry after alert dismissal.
        :param delay: Base delay (seconds) before retry; jitter added for realism.
        :return: Result of callable_obj if successful.
        :raises: Last exception if unrecoverable.
        """
        attempt = 0
        last_exc = None
        while attempt <= retries:
            try:
                return callable_obj()
            except UnexpectedAlertPresentException as e:
                last_exc = e
                self.dismiss_alert()
                # Jittered delay (e.g., 0.25s +/- up to ~33%)
                time.sleep(delay + random.uniform(0, delay/3))
                attempt += 1
                continue
            except Exception:
                # Non-alert exceptions propagate immediately (caller decides handling)
                raise
        if last_exc:
            raise last_exc

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
        if self.browser == 'firefox':
            primary_url = "about:blank"
            fallback_url = "data:,"
        else:
            raise NotImplementedError("Currently only Firefox is supported")

        # Attempt to abort any hanging network activity first; ignore failures.
        try:
            self.driver.execute_script("window.stop();")
        except Exception:
            pass

        def _navigate(url):
            return self.safe_action(lambda: self.driver.get(url))

        try:
            _navigate(primary_url)
        except TimeoutException as e_primary:
            # Log & fallback to secondary reset URL
            try:
                if self.selenium_log:
                    self.selenium_log.warning(f"Timeout navigating to {primary_url} during reset: {e_primary}; trying fallback {fallback_url}")
                _navigate(fallback_url)
            except TimeoutException as e_fallback:
                # Escalate: restart selenium; this indicates a likely hung browser
                if self.selenium_log:
                    self.selenium_log.error(f"Fallback reset navigation also timed out ({fallback_url}): {e_fallback}; restarting browser session")
                try:
                    self.restart_selenium()
                except Exception as restart_err:
                    if self.selenium_log:
                        self.selenium_log.error(f"Failed to restart Selenium after reset failure: {restart_err}")
                    raise
                
                # After restart, attempt once more (primary only) but swallow errors to avoid cascading failures
                try:
                    _navigate(primary_url)
                except Exception:
                    pass
        finally:
            try:
                self.last_scraped_url = self.driver.current_url
            except Exception:
                self.last_scraped_url = None

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
                    if alt_text and isinstance(alt_text, str):
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
        def _scroll_down_page_to_load():
            def _scroll_to_top():
                try:
                    self.driver.execute_script("window.scrollTo(0, 0);")
                except JavascriptException:
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
                except JavascriptException:
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
        
        return self.safe_action(_scroll_down_page_to_load)

    def kill_browser(self):
        try:
            # Prefer current driver PID if available
            if self.driver is None or self.driver.service is None or self.driver.service.process is None or self.driver.service.process.pid is None:
                if self.browser_pid:
                    pid = self.browser_pid
                else:
                    self.selenium_log.warning(f"Trying to kill {self.browser}, but unable to determine PID")
                    return
            else:
                pid = self.driver.service.process.pid  # geckodriver/chromedriver PID
            self.selenium_log.info(f"4CAT is killing {self.browser} with PID: {pid}")
            try:
                pgid = os.getpgid(pid)
                # Kill the whole group (geckodriver + firefox children)
                os.killpg(pgid, signal.SIGTERM)
                time.sleep(2)
                # If still alive, force kill
                try:
                    os.killpg(pgid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            except Exception:
                # Fallback to direct PID kill
                subprocess.check_call(['kill', str(pid)])
        except subprocess.CalledProcessError as e:
            self.selenium_log.error(f"Error killing {self.browser} (PID: {pid}): {e}")

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
            "help": "Max site per dataset",
            "coerce_type": int,
            "tooltip": "Maximum number of sites to scrape per dataset; can be overwritten by specific processors",
        },
        "selenium.selenium_executable_path": {
            "type": UserInput.OPTION_TEXT,
            "default": "/usr/local/bin/geckodriver",
            "help": "Path to webdriver (geckodriver or chromedriver)",
            "tooltip": "fourcat_install.py installs to /usr/local/bin/geckodriver",
        },
        "selenium.firefox_binary_path":{
            "type": UserInput.OPTION_TEXT,
            "default": None,
            "help": "Path to Firefox binary",
            "tooltip": "Selenium will attempt to locate the Firefox binary automatically if set to `None`.",
        },
        "selenium.firefox_profile_path":{
            "type": UserInput.OPTION_TEXT,
            "default": None,
            "help": "Path to Firefox profile",
            "tooltip": "`None` will create a temporary profile each startup.",
        },
        "selenium.page_load_timeout": {
            "type": UserInput.OPTION_TEXT,
            "help": "Default time to wait for page load",
            "default": 60,
            "coerce_type": int,
            "tooltip": "May be overwritten by specific processors"
        },
        "selenium.implicit_wait": {
            "type": UserInput.OPTION_TEXT,
            "help": "Time to wait for elements to appear",
            "default": 10,
            "coerce_type": int,
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
        "selenium.use_virtual_display": {
            "type": UserInput.OPTION_TOGGLE,
            "default": False,
            "help": "Use virtual display (Xvfb) if available",
            "tooltip": "Use virtual display (Xvfb) if available; otherwise, headless mode is used",
        },
        "selenium.reduce_dialog_prefs": {
            "type": UserInput.OPTION_TOGGLE,
            "default": False,
            "help": "Apply Firefox prefs to suppress notification / push / beforeunload dialogs",
            "tooltip": "Disable to mimic a more default browser profile if detection is suspected.",
        },
        "selenium.unhandled_prompt_behavior": {
            "type": UserInput.OPTION_TEXT,
            "default": "dismiss",
            "help": "Unhandled JS dialog strategy (dismiss, accept, dismiss and notify, accept and notify)",
            "tooltip": "Internal WebDriver capability; not directly visible to page JS.",
        },
        
    }
    
    @classmethod
    def get_queue_id(cls, remote_id, details, dataset) -> str:
        """
        Get queue ID for this worker

        The queue ID determines what other worker types are considered to see
        if a job of this worker can run. By default it is set to the worker's
        type (so all workers of the same type are in the same queue) but this
        can be overridden by subclasses.

        :param str remote_id:  Item reference for the job, e.g. a dataset key
          or URL
        :param dict details:  Job details
        :param DataSet dataset:  Dataset object; if the worker does not work
          with a dataset (e.g. if it is a processor) this is `None`.

	    :return str:  Queue ID
	    """
        return "selenium_search"

    def search(self, query):
        """
        Search for items matching the given query

        The real work is done by the get_items() method of the descending
        class. This method just provides some scaffolding and post-processing
        of results via `after_search()`, if it is defined.

        :param dict query:  Query parameters
        :return:  Iterable of matching items, or None if there are no results.
        """
        start = time.time()
        self.dataset.log(f"Checking for selenium {time.time() - start:.2f} seconds")
        if not self.is_selenium_available(config=self.config):
            raise ProcessorException("Selenium not available; please ensure browser and webdriver are installed and configured in settings")
        
        try:
            self.start_selenium(eager=self.eager_selenium)
        except ProcessorException as e:
            self.quit_selenium()
            raise e
        self.dataset.log(f"Started selenium and {self.browser} ({time.time() - start:.2f} seconds)")
        # Returns to default position; i.e., 'data:,'
        try:
            self.reset_current_page()
        except InvalidSessionIdException as e:
            # Webdriver unable to connect to browser
            self.log.error(f"InvalidSessionIdException: {e}")
            self.quit_selenium()
            raise ProcessorException("Selenium or browser unable to start; please wait and try again later")

        self.dataset.log("Collecting posts...")
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
