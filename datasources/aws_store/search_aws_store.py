from datetime import datetime, timedelta
import re
import json
import urllib
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.common import exceptions as selenium_exceptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.expected_conditions import staleness_of

from extensions.web_studies.selenium_scraper import SeleniumWrapper
from backend.lib.worker import BasicWorker
from common.lib.exceptions import ProcessorInterruptedException, ProcessorException, QueryNeedsExplicitConfirmationException
from common.lib.item_mapping import MappedItem
from common.lib.user_input import UserInput
from extensions.web_studies.selenium_scraper import SeleniumSearch
from common.lib.helpers import url_to_hash


class SearchAwsStore(SeleniumSearch):
    """
    Search Amazon Web Services Marketplace data source
    """
    type = "aws-store-search"  # job ID
    category = "Search"  # category
    title = "Amazon Web Services (AWS) Marketplace"  # title displayed in UI
    description = "Query Amazon Web Services Marketplace to retrieve data on applications and developers"  # description displayed in UI
    extension = "ndjson"  # extension of result file, used internally and in UI
    is_local = False  # Whether this datasource is locally scraped
    is_static = False  # Whether this datasource is still updated

    config = {
        "cache.aws.query_options": {
            "type": UserInput.OPTION_TEXT_JSON,
            "help": "AWS Query Options",
            "tooltip": "automatically updated",
            "default": {},
            "indirect": True
        },
        "cache.aws.query_options_updated_at": {
            "type": UserInput.OPTION_TEXT,
            "help": "AWS Query Options Updated At",
            "tooltip": "automatically updated",
            "default": 0,
            "coerce_type": float,
            "indirect": True
        }
    }

    base_url = "https://aws.amazon.com/marketplace/"
    search_url = base_url + "search/"
    max_results = 1000
    query_param_map = {
        "Categories": "category",
        "Vendors": "creator",
        "Pricing Models": "pricing_model",
        "Delivery Methods": "fulfillment_option_type",
    }
    query_param_ignore = ['All pricing models', 'All delivery methods', 'All vendors', 'All categories', 'Show all +1000 vendors']


    @classmethod
    def is_compatible_with(cls, module=None, config=None):
        """
        Allow if Selenium is available
        """
        return SeleniumSearch.is_selenium_available(config=config)

    @classmethod
    def get_options(cls, parent_dataset=None, config=None):
        max_results = cls.max_results
        options = {
            "intro-1": {
                "type": UserInput.OPTION_INFO,
                "help": (
                    "This data source allows you to query [Amazon Web Services Marketplace](https://aws.amazon.com/marketplace/) to retrieve data on applications and developers."
                    )
            },
            "amount": {
                "type": UserInput.OPTION_TEXT,
                "help": "Max number of results per query" + (f" (max {max_results:,})" if max_results != 0 else ""),
                "default": 60 if max_results == 0 else min(max_results, 60),
                "min": 0 if max_results == 0 else 1,
                "max": max_results,
                "tooltip": "The AWS Marketplace returns apps in batches of 20."
            },
            "search_query": {
                "type": UserInput.OPTION_TEXT_LARGE,
                "help": "List of queries to search (leave blank for all).",
                "default": "",  # need default else regex will fail
            },
            "advanced_filters": {
                "type": UserInput.OPTION_TOGGLE,
                "help": "Advanced Filters",
                "default": False,
                "tooltip": "Enable advanced filters to filter by category, creator, pricing model, and delivery method."
            }
        }

        # Query Options collected every day by AwsStoreCategories worker below
        filter_options = config.get("cache.aws.query_options", {})
        for filter_name, filter_options in filter_options.items():
            if filter_name not in cls.query_param_map:
                config.db.log.warning(f"AWS Unknown filter name: {filter_name}")
                continue

            if not filter_options:
                # Some filters are not currently being collected due to UI and API changes
                continue

            options[cls.query_param_map[filter_name]] = {
                "type": UserInput.OPTION_MULTI_SELECT,
                "help": f"Filter by {filter_name}",
                "options": {(option["name"] if option["name"] not in cls.query_param_ignore else "all"): option["name"] for option in filter_options},
                "default": "all",
                "requires": "advanced_filters==true",
            }

        # TODO: add full details collection
        # options["full_details"] = {
        #         "type": UserInput.OPTION_TOGGLE,
        #         "help": "Include full application details",
        #         "default": False,
        #         "tooltip": "If enabled, the full details of each application will be included in the output.",
        #     }
        return options

    def get_items(self, query):
        """
        Fetch items from AWS Store

        :param query:
        :return:
        """
        if not self.is_selenium_available(config=self.config):
            self.dataset.update_status("Selenium not available; unable to collect from AWS Store.", is_final=True)
            return

        search_queries = query.get('search_query', [])
        if not search_queries:
            # can search all
            search_queries = [""]
        max_results = int(query.get('amount', 60))
        full_details = query.get('full_details', False)
        categories = query.get('category', ["all"])
        creators = query.get('creator', ["all"])
        pricing_models = query.get('pricing_model', ["all"])
        fulfillment_option_types = query.get('fulfillment_option_type', ["all"])

        # Filter mappings
        filter_options = {}
        all_filters = self.config.get("cache.aws.query_options", {})
        for filter_name, filters in all_filters.items():
            if filter_name not in self.query_param_map:
                self.log.warning(f"AWS Unknown filter name ({self.dataset.key}): {filter_name}")
                continue
            
            filter_options[self.query_param_map[filter_name]] = {option["name"]: option["data-value"] for option in filters if option["name"] not in self.query_param_ignore}

        missing_filters = []
        total_queries = len(search_queries) * len(categories) * len(creators) * len(pricing_models) * len(fulfillment_option_types)
        self.dataset.update_status(f"Collecting {total_queries} queries (max {max_results} per query) from AWS Store")
        tried_queries = 0
        for search_query in search_queries:
            for category in categories:
                if category == "all":
                    category_code = None
                else:
                    category_code = filter_options.get("category", {}).get(category)
                    if not category_code:
                        self.dataset.update_status(f"Category code not found for {category}")
                        missing_filters.append(category)
                        continue

                for creator in creators:
                    if creator == "all":
                        creator_code = None
                    else:
                        creator_code = filter_options.get("creator", {}).get(creator)
                        if not creator_code:
                            self.dataset.update_status(f"Creator code not found for {creator}")
                            missing_filters.append(creator)
                            continue

                    for pricing_model in pricing_models:
                        if pricing_model == "all":
                            pricing_model_code = None
                        else:
                            pricing_model_code = filter_options.get("pricing_model", {}).get(pricing_model)
                            if not pricing_model_code:
                                self.dataset.update_status(f"Pricing model code not found for {pricing_model}")
                                missing_filters.append(pricing_model)
                                continue

                        for fulfillment_option_type in fulfillment_option_types:
                            tried_queries += 1
                            if fulfillment_option_type == "all":
                                fulfillment_option_type_code = None
                            else:
                                fulfillment_option_type_code = filter_options.get("fulfillment_option_type", {}).get(fulfillment_option_type)
                                if not fulfillment_option_type_code:
                                    self.dataset.update_status(f"Fulfillment option type code not found for {fulfillment_option_type}")
                                    missing_filters.append(fulfillment_option_type)
                                    continue

                            if self.interrupted:
                                raise ProcessorInterruptedException("Interrupted while collecting AWS Store queries")
                            collected = 0
                            page = 1
                            result_number = 1
                            query_url = self.get_query_url(self.search_url,
                                                        query=search_query if search_query else None,
                                                        category=category_code,
                                                        creator=creator_code,
                                                        pricing_model=pricing_model_code,
                                                        fulfillment_option_type=fulfillment_option_type_code)
                            success, errors = self.get_with_error_handling(query_url)
                            if not success:
                                self.dataset.log(f"Unable to collect AWS page {query_url}: {errors}")
                                continue
                            else:
                                self.dataset.log(f"Successfully retrieved AWS page {query_url}")

                            try:
                                text_results = WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH, '//span[@data-test-selector="availableProductsCountMessage"]')))
                                text_results = text_results.text.lstrip('(').rstrip(" results)")
                                try:
                                    num_results = int(text_results.replace("Over ", "").replace(",", ""))
                                except ValueError:
                                    num_results = None
                                    self.log.warning(f"{self.type} ({self.dataset.key}) could not parse number of results: {text_results}")
                                if num_results == 0:
                                    self.dataset.log(f"No results found{', continuing...' if tried_queries < (total_queries - 1) else ''}")
                                    continue
                                else:
                                    self.dataset.log(f"Found total of {num_results if (num_results and 'over' not in text_results.lower()) else text_results.lower()} results")
                            except selenium_exceptions.NoSuchElementException:
                                num_results = None
                                self.log.warning(f"{self.type} number of results element not found; unknown number of results")
                                self.dataset.log("Unknown number of results found")
                            total_results = min(num_results if num_results else max_results, max_results)
                            self.dataset.log(f"Starting AWS pagination at page {page}; target {total_results} results")

                            while collected < total_results:
                                try:
                                    results_table = WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, 'tbody')))
                                    WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH, '//h2[@data-semantic="title"]')))
                                except selenium_exceptions.TimeoutException:
                                    self.dataset.log(f"AWS page {page}: timeout waiting for results to render; stopping pagination for this query")
                                    break

                                # Check if there are results
                                result_rows = results_table.find_elements(By.TAG_NAME, "tr")
                                self.dataset.log(f"AWS page {page}: found {len(result_rows)} rows")
                                if not result_rows:
                                    self.dataset.log(f"No result rows found on AWS page {page}; stopping pagination for this query")
                                    break
                                else:
                                    self.dataset.log(f"AWS page {page}: collecting results table w/ {len(result_rows)} rows ({collected}/{total_results} collected previously)")

                                default_implicit_wait = self.config.get('selenium.implicit_wait', 10)
                                # Set implicit wait to 0 to speed up collection since we are already checking for element presence with WebDriverWait; will reset to default at end of page collection
                                self.driver.implicitly_wait(0)
                                try:
                                    for row_index, result_block in enumerate(result_rows, start=1):
                                        if self.interrupted:
                                            raise ProcessorInterruptedException("Interrupted while collecting AWS Store results")

                                        if collected >= total_results:
                                            break

                                        # TODO: check full details
                                        try:
                                            result = self.parse_search_result(result_block)
                                        except Exception as e:
                                            self.dataset.log(f"AWS page {page}: failed parsing row {row_index}; skipping ({type(e).__name__}: {e})")
                                            continue

                                        result["id"] = result["app_id"]
                                        result["4CAT_metadata"] = {"query": search_query,
                                                                "category": category,
                                                                "creator": creator,
                                                                "pricing_model": pricing_model,
                                                                "fulfillment_option_type": fulfillment_option_type,
                                                                # These codes are used to filter the results in the AWS Store
                                                                "filter_codes": {"category": category_code,
                                                                                "creator": creator_code,
                                                                                "pricing_model": pricing_model_code,
                                                                                "fulfillment_option_type": fulfillment_option_type_code},
                                                                "page": page,
                                                                "rank": result_number,
                                                                "collected_at_timestamp": datetime.now().timestamp()}
                                        result_number += 1
                                        collected += 1
                                        yield result
                                finally:
                                    self.driver.implicitly_wait(default_implicit_wait)

                                self.dataset.log(f"Collected {collected} results so far for query: {search_query if search_query else 'no query provided'}")
                                first_row_before = result_rows[0]
                                if not self.click_next_page(self.driver):
                                    # No next page
                                    break
                                else:
                                    try:
                                        WebDriverWait(self.driver, 5).until(staleness_of(first_row_before))
                                    except selenium_exceptions.TimeoutException:
                                        self.dataset.log(f"AWS page did not change after clicking next on page {page}; stopping pagination for this query")
                                        break
                                    page += 1

                            if not collected:
                                self.dataset.log(f"AWS no-rows debug URL: {self.driver.current_url}")
                                try:
                                    body_text = self.driver.find_element(By.TAG_NAME, "body").text.strip()
                                except selenium_exceptions.NoSuchElementException:
                                    body_text = ""
                                if body_text:
                                    self.dataset.log(f"AWS no-rows debug text: {body_text[:1000].replace(chr(10), ' ')}")
                                else:
                                    self.dataset.log(f"AWS no-rows debug source: {self.driver.page_source[:1500].replace(chr(10), ' ')}")
                            
                            self.dataset.update_progress(tried_queries / total_queries)
                            self.dataset.update_status(f"Collected {collected} of {total_results} results for query: {search_query if search_query else 'no query provided'}, category: {category if category else 'all'}, creator: {creator if creator else 'all'}, pricing model: {pricing_model_code if pricing_model_code else 'all'}, fulfillment type: {fulfillment_option_type_code if fulfillment_option_type_code else 'all'} ({tried_queries} of {total_queries})")

        if missing_filters:
            self.dataset.log(f"Missing filter codes needed to search for: {', '.join(missing_filters)}")
            self.dataset.update_status(f"Not all filters could be found; see log for details", is_final=True)

    @staticmethod
    def parse_search_result(result_element):
        """
        Parse search result Selenium element for useful data

        TODO: could be moved to map item, but would need to parse without Selenium (e.g., BeautifulSoup); would still
        need link for full details regardless

        :param result_element:  Selenium element
        :return:  dict
        """
        # icon
        try:
            thumbnail = result_element.find_element(By.XPATH, './/div[@data-semantic="logo"]').find_element(By.TAG_NAME,"img").get_attribute("src")
        except selenium_exceptions.NoSuchElementException:
            thumbnail = None
        # app title
        title_block = result_element.find_element(By.XPATH, './/h2[@data-semantic="title"]')
        title = title_block.text
        app_url = title_block.find_element(By.TAG_NAME, "a").get_attribute("href")
        app_id = app_url.split("prodview-")[1].split("?")[0]
        # vendor
        try:
            vendor_block = result_element.find_element(By.XPATH, './/a[@data-semantic="vendorNameLink"]')
            vendor_name = vendor_block.text
            vendor_url = vendor_block.get_attribute("href")
        except selenium_exceptions.NoSuchElementException:
            # vendor not linked
            try:
                vendor_block = result_element.find_element(By.XPATH, './/span[@data-semantic="vendorNameLink"]')
                vendor_name = vendor_block.text
                vendor_url = None
            except selenium_exceptions.NoSuchElementException:
                vendor_name = None
                vendor_url = None
        # pricing
        try:
            badge = result_element.find_element(By.XPATH, './/span[@data-semantic="badge-text"]').text
        except selenium_exceptions.NoSuchElementException:
            badge = None
        try:
            pricing = result_element.find_element(By.XPATH, './/div[@data-semantic="pricing"]').text
        except selenium_exceptions.NoSuchElementException:
            pricing = None

        # 2026-2-25 AWS updated their UI and added a new "AI listing highlights" section which often in place of the description itself
        # description
        try:
            search_description = result_element.find_element(By.XPATH, './/p[@data-semantic="desc"]').text
        except selenium_exceptions.NoSuchElementException:
            search_description = None

        # ai highlights
        try:
            ai_listing_highlights = result_element.find_element(By.XPATH, './/div[@data-semantic="ai-listing-highlights"]').text
        except selenium_exceptions.NoSuchElementException:
            ai_listing_highlights = None

        return {
            "app_id": app_id,
            "title": title,
            "app_url": app_url,
            "vendor_name": vendor_name,
            "vendor_url": vendor_url,
            "badge": badge,
            "pricing": pricing,
            "search_description": search_description,
            "thumbnail": thumbnail,
            "ai_listing_highlights": ai_listing_highlights,
            "html_source": result_element.get_attribute("outerHTML"),
        }

    @staticmethod
    def get_query_url(url, query=None, category=None, creator=None, pricing_model=None, fulfillment_option_type=None):
        filters = []
        params = {}
        if query:
            params["searchTerms"] = query
        if category:
            params["category"] = category
        if creator:
            params["CREATOR"] = creator
            filters.append("CREATOR")
        if pricing_model:
            params["PRICING_MODEL"] = pricing_model
            filters.append("PRICING_MODEL")
        if fulfillment_option_type:
            params["FULFILLMENT_OPTION_TYPE"] = fulfillment_option_type
            filters.append("FULFILLMENT_OPTION_TYPE")
        if filters:
            params["filters"] = ",".join(filters)
        url += "?" + urllib.parse.urlencode(params) if params else ""
        return url

    def click_next_page(self, driver):
        """"
        Click next page button
        """
        next_page = driver.find_elements(By.XPATH, '//button[@aria-label="Next page"]')
        if not next_page:
            return False

        next_button = next_page[0]
        if next_button.get_attribute("aria-disabled") == "true" or not next_button.is_enabled():
            return False

        driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
        return self.smart_click(next_button)

    @staticmethod
    def map_item(item):
        """
        Map item to a standard format
        """
        fourcat_metadata = item.pop("4CAT_metadata", {})
        # Remove HTML source
        item.pop("html_source")
        best_text = item.get("search_description") or item.get("ai_listing_highlights") or ""
        return MappedItem({
            "query": fourcat_metadata.get("query", ""),
            "category": fourcat_metadata.get("category") if fourcat_metadata.get("category") else "all",
            "creator": fourcat_metadata.get("creator") if fourcat_metadata.get("creator") else "all",
            "pricing_model": fourcat_metadata.get("pricing_model") if fourcat_metadata.get("pricing_model") else "all",
            "fulfillment_option_type": fourcat_metadata.get("fulfillment_option_type") if fourcat_metadata.get("fulfillment_option_type") else "all",
            "page": fourcat_metadata.get("page", ""),
            "rank": fourcat_metadata.get("rank", ""),
            "timestamp": int(fourcat_metadata.get("collected_at_timestamp")),
            "body": best_text,
            **item
        })

    @staticmethod
    def validate_query(query, request, config):
        """
        Validate input for a dataset query on the data source.

        Will raise a QueryParametersException if invalid parameters are
        encountered. Parameters are additionally sanitised.

        :param dict query:  Query parameters, from client-side.
        :param request:  Flask request
        :param User user:  User object of user who has submitted the query
        :return dict:  Safe query parameters
        """
        queries = [q.strip() for q in re.split(',|\n', query.get('search_query', "")) if q.strip()]
        if not queries:
            # always at least one "query"
            queries = [""]
        category = query.get('category', ["all"])
        if type(category) == str:
            category = [category]
        creator =query.get('creator', ["all"])
        if type(creator) == str:
            creator = [creator]
        pricing_model = query.get('pricing_model', ["all"])
        if type(pricing_model) == str:
            pricing_model = [pricing_model]
        fulfillment_option_type = query.get('fulfillment_option_type', ["all"])
        if type(fulfillment_option_type) == str:
            fulfillment_option_type = [fulfillment_option_type]
        
        total_queries = len(queries) * len(category) * len(creator) * len(pricing_model) * len(fulfillment_option_type)
        num_results = int(query.get('amount', 60)) * total_queries

        if not query.get("frontend-confirm") and (total_queries > 50 or num_results > 10000):
            raise QueryNeedsExplicitConfirmationException(f"This combination of filters and queries will result in {total_queries} queries w/ up to {num_results} results. Please confirm you want to proceed.")
        return {
            "search_query": queries,
            "category": category,
            "creator": creator,
            "pricing_model": pricing_model,
            "fulfillment_option_type": fulfillment_option_type,
            "amount": query.get('amount', 60),
        }


class AwsStoreCategories(BasicWorker):
    """
    Collect AWS Store categories and store them in database
    """
    type = "aws-store-category-collector"  # job ID

    @classmethod
    def ensure_job(cls, config=None):
        """
        Ensure job is scheduled to run every day
        """
        # Run every day to update categories
        if "aws-store" in config.get("datasources.enabled"):
            return {"remote_id": "aws-store-category-collector", "interval": 86400}
        return None

    def work(self):
        """
        Collect AWS Store query options and store them in database via Selenium
        """
        categories_url = SearchAwsStore.base_url
        selenium_wrapper = SeleniumWrapper()
        if not selenium_wrapper.is_selenium_available(config=self.config):
            raise ProcessorException("Selenium is not available; cannot collect categories from AWS Store")

        selenium_wrapper.start_selenium(config=self.config)
        # Backend runs get_options for each processor on init; but does not seem to have logging
        selenium_wrapper.selenium_log.info(f"Fetching category options from AWS Store {categories_url}")
        selenium_wrapper.driver.get(categories_url)
        if not selenium_wrapper.check_for_movement():
            raise ProcessorException("Failed to load AWS Store")

        if not selenium_wrapper.check_page_is_loaded():
            raise ProcessorException("AWS Store did not load and timed out")

        selenium_wrapper.scroll_down_page_to_load(60)

        try:
            category_filters = self.get_category_filters(selenium_wrapper=selenium_wrapper, logger=self.log)
            if category_filters:
                self.log.info(f"Collected {len(category_filters)} query types with a total of {len(sum(category_filters.values(), []))} options for the AWS Store")
                # Update existing options with new data (replace whole sections to remove old options, but do not remove sections that are no longer collected at all due to UI/API changes)
                old_options = self.config.get("cache.aws.query_options", {})
                for filter_name, options in category_filters.items():
                    old_options[filter_name] = options

                self.config.set("cache.aws.query_options", old_options)
                self.config.set("cache.aws.query_options_updated_at", datetime.now().timestamp())
            else:
                self.log.warning("Failed to collect category options from AWS Store")

        except ProcessorException as e:
            self.log.error(f"Error collecting AWS Store categories: {e}")
        finally:
            # Always quit selenium
            selenium_wrapper.quit_selenium()

        return

    @staticmethod
    def get_category_filters(selenium_wrapper, logger):
        """
        Get category filters from AWS Store

        :param selenium_wrapper:  SeleniumWrapper
        :param logger:  Logger
        :return:  dict
        """
        # 2026-3: AWS updated UI and filters no longer exist in dropdowns, Vendors, Pricing Models, and Delivery Methods filters are now text-based with "Show all" options and only on search pages
        # TODO: if required, add navigation and parse these from search pages; vendors (now publishers?) have show all and popup page with next buttons as well

        # Extract categories from embedded JSON
        soup = BeautifulSoup(selenium_wrapper.driver.page_source, "html.parser")
        markers = [
            ("var categoryMap = ", r"var categoryMap\s*="),
        ]
        category_data = AwsStoreCategories.parse_category_json(soup, markers=markers, log=logger, debug=True)
        new_data = {
            "Categories":[{'name': 'All categories', 'data-value': 'All categories'}],
        }
        # Looks like all we need are the english names and the data-value
        for i, (k,v) in enumerate(category_data['categoryMessageMap']['en'].items()):
            new_data["Categories"].append({"name": v, "data-value": k})
        
        return new_data

    @staticmethod    
    def parse_category_json(soup, markers=None, log=None, debug=False):
        """
        Parse JSON object from AWS Store.

        Pass `debug=True` and `log=<logger>` to enable detailed diagnostics.
        """
        scripts = soup.find_all("script")
        if debug and log:
            log.info(f"Parsing JSON data from AWS Store, found {len(scripts)} script tags to check")

        decoder = json.JSONDecoder()
        for i, script in enumerate(scripts):
            script_text = script.string or script.get_text() or script.decode_contents() or ""
            if not script_text:
                continue

            marker_used = None
            marker_match = None
            for marker_name, marker_pattern in markers:
                match = re.search(marker_pattern, script_text)
                if match:
                    marker_used = marker_name
                    marker_match = match
                    break

            if not marker_match:
                continue

            if debug and log:
                log.info(f"Script[{i}] contains {marker_used}; first 180 chars: {script_text[:180]!r}")

            rhs = script_text[marker_match.end():].lstrip()

            # Find first JSON object start
            brace_pos = rhs.find("{")
            if brace_pos == -1:
                if debug and log:
                    log.warning(f"Found {marker_used} in Script[{i}] but no '{{' after assignment")
                continue

            try:
                parsed, _ = decoder.raw_decode(rhs[brace_pos:])
                if debug and log:
                    log.info(f"Successfully parsed JSON data from AWS Store using {marker_used} in Script[{i}]")
                return parsed
            except json.JSONDecodeError as e:
                if debug and log:
                    log.error(f"Failed JSON decode for Script[{i}] with {marker_used}: {e}")
                continue

        if log:
            log.error("Failed to find category JSON data from AWS Store")
        return None