from datetime import datetime, timedelta
import re
import urllib
from selenium.webdriver.common.by import By
from selenium.common import exceptions as selenium_exceptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from extensions.web_studies.selenium_scraper import SeleniumWrapper
from backend.lib.worker import BasicWorker
from common.lib.exceptions import ProcessorInterruptedException, ProcessorException
from common.lib.item_mapping import MappedItem
from common.lib.user_input import UserInput
from extensions.web_studies.selenium_scraper import SeleniumSearch
from common.lib.helpers import url_to_hash

from common.config_manager import config


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
    def is_compatible_with(cls, module=None, user=None):
        """
        Allow if Selenium is available
        """
        return SeleniumSearch.is_selenium_available()

    @classmethod
    def get_options(cls, parent_dataset=None, user=None):
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
                "help": "Max number of results" + (f" (max {max_results:,})" if max_results != 0 else ""),
                "default": 60 if max_results == 0 else min(max_results, 60),
                "min": 0 if max_results == 0 else 1,
                "max": max_results,
                "tooltip": "The AWS Marketplace returns apps in batches of 20."
            },
            "query": {
                "type": UserInput.OPTION_TEXT_LARGE,
                "help": "List of queries to search (leave blank for all).",
                "default": "",  # need default else regex will fail
            }
        }

        # Query Options collected every day by AwsStoreCategories worker below
        filter_options = config.get("cache.aws.query_options", {})
        for filter_name, filter_options in filter_options.items():
            if filter_name not in cls.query_param_map:
                config.db.log.warning(f"AWS Unknown filter name: {filter_name}")
                continue

            options[cls.query_param_map[filter_name]] = {
                "type": UserInput.OPTION_CHOICE,
                "help": f"Filter by {filter_name}",
                "options": {(option["data-value"] if option["name"] not in cls.query_param_ignore else "all"): option["name"] for option in filter_options},
                "default": "all",
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
        if not self.is_selenium_available():
            self.dataset.update_status("Selenium not available; unable to collect from AWS Store.", is_final=True)
            return

        queries = re.split(',|\n', self.parameters.get('query', ''))
        if not queries:
            # can search all
            queries = [None]
        max_results = int(self.parameters.get('amount', 60))
        full_details = self.parameters.get('full_details', False)
        category = self.parameters.get('category', None) if self.parameters.get('category', None) != "all" else None
        creator = self.parameters.get('creator', None) if self.parameters.get('creator', None) != "all" else None
        pricing_model = self.parameters.get('pricing_model', None) if self.parameters.get('pricing_model', None) != "all" else None
        fulfillment_option_type = self.parameters.get('fulfillment_option_type', None) if self.parameters.get('fulfillment_option_type', None) != "all" else None

        collected = 0
        for i, query in enumerate(queries):
            if self.interrupted:
                raise ProcessorInterruptedException("Interrupted while collecting AWS Store queries")

            self.dataset.update_status(f"Collecting AWS Store data for query: {query} ({i+1} of {len(queries)})")
            page = 1
            result_number = 1
            query_url = self.get_query_url(self.search_url,
                                           query=query if query else None,
                                           category=category,
                                           creator=creator,
                                           pricing_model=pricing_model,
                                           fulfillment_option_type=fulfillment_option_type)
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
                    num_results = int(text_results.replace("Over ", ""))
                except ValueError:
                    num_results = None
                    self.log.warning(f"{self.type} could not parse number of results: {text_results}")
                if num_results == 0:
                    self.dataset.log(f"No results found{', continuing...' if i < len(queries) - 1 else ''}")
                    continue
                else:
                    self.dataset.log(f"Found total of {num_results if (num_results and 'over' not in text_results.lower()) else text_results.lower()} results")
            except selenium_exceptions.NoSuchElementException:
                num_results = None
                self.log.warning(f"{self.type} number of results element not found; unknown number of results")
                self.dataset.log("Unknown number of results found")
            total_results = min(num_results if num_results else max_results, max_results)

            while collected < max_results:
                results_table = WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.TAG_NAME, 'tbody')))
                # Wait for first result to load
                WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH, '//h2[@data-semantic="title"]')))

                for result_block in results_table.find_elements(By.TAG_NAME, "tr"):
                    if self.interrupted:
                        raise ProcessorInterruptedException("Interrupted while collecting AWS Store results")

                    # TODO: check full details
                    result = self.parse_search_result(result_block)
                    result["id"] = url_to_hash(result["app_id"])
                    result["4CAT_metadata"] = {"query": query,
                                               "category": category,
                                               "creator": creator,
                                               "pricing_model": pricing_model,
                                               "fulfillment_option_type": fulfillment_option_type,
                                               "page": page,
                                               "rank": result_number,
                                               "collected_at_timestamp": datetime.now().timestamp()}
                    result_number += 1
                    collected += 1
                    yield result
                    self.dataset.update_progress(collected / total_results)
                    self.dataset.update_status(
                        f"Collected {collected} of {total_results} results for query: {query} ({i+1} of {len(queries)})")

                if not self.click_next_page(self.driver):
                    # No next page
                    break
                else:
                    page += 1

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
        vendor_block = result_element.find_element(By.XPATH, './/a[@data-semantic="vendorNameLink"]')
        vendor_name = vendor_block.text
        vendor_url = vendor_block.get_attribute("href")
        # pricing
        try:
            badge = result_element.find_element(By.XPATH, './/span[@data-semantic="badge-text"]').text
        except selenium_exceptions.NoSuchElementException:
            badge = None
        try:
            pricing = result_element.find_element(By.XPATH, './/div[@data-semantic="pricing"]').text
        except selenium_exceptions.NoSuchElementException:
            pricing = None
        # description
        search_description = result_element.find_element(By.XPATH, './/p[@data-semantic="desc"]').text
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

    @staticmethod
    def click_next_page(driver):
        """"
        Click next page button
        """
        next_page = driver.find_elements(By.XPATH, '//button[@aria-label="Next page"]')
        if not next_page:
            return False
        driver.execute_script("arguments[0].scrollIntoView(true);", next_page[0])
        next_page[0].click()
        return True

    @staticmethod
    def map_item(item):
        """
        Map item to a standard format
        """
        item["body"] = item["search_description"]
        fourcat_metadata = item.pop("4CAT_metadata", {})
        # Remove HTML source
        item.pop("html_source")
        return MappedItem({
            "query": fourcat_metadata.get("query", ""),
            "page": fourcat_metadata.get("page", ""),
            "rank": fourcat_metadata.get("rank", ""),
            "timestamp": fourcat_metadata.get("collected_at_timestamp", ""),
            **item
        })

    @staticmethod
    def validate_query(query, request, user):
        """
        Validate input for a dataset query on the data source.

        Will raise a QueryParametersException if invalid parameters are
        encountered. Parameters are additionally sanitised.

        :param dict query:  Query parameters, from client-side.
        :param request:  Flask request
        :param User user:  User object of user who has submitted the query
        :return dict:  Safe query parameters
        """

        return query


class AwsStoreCategories(BasicWorker):
    """
    Collect AWS Store categories and store them in database
    """
    type = "aws-store-category-collector"  # job ID

    # Run every day to update categories
    ensure_job = {"remote_id": "aws-store-category-collector", "interval": 86400}

    def work(self):
        """
        Collect AWS Store query options and store them in database via Selenium
        """
        categories_url = SearchAwsStore.base_url
        selenium_wrapper = SeleniumWrapper()
        if not selenium_wrapper.is_selenium_available():
            raise ProcessorException("Selenium is not available; cannot collect categories from AWS Store")

        # Backend runs get_options for each processor on init; but does not seem to have logging
        selenium_wrapper.selenium_log.info(f"Fetching category options from AWS Store {categories_url}")

        selenium_wrapper.start_selenium()
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
                config.set("cache.aws.query_options", category_filters)
                config.set("cache.aws.query_options_updated_at", datetime.now().timestamp())
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
        # Get Query options
        search_container_id = "migration_picker_internal_container"
        search_container = selenium_wrapper.driver.find_element(By.ID, search_container_id)
        option_containers = search_container.find_elements(By.TAG_NAME, "awsui-select")
        # Collect possible filter options
        query_filters = {}
        for option_container in option_containers:
            option_name = option_container.find_element(By.XPATH, "../span").text
            query_filters[option_name] = []

            # Open option dropdown
            button = option_container.find_elements(By.CLASS_NAME, "awsui-select-trigger-icon")
            if not button:
                logger.warning(f"Unable to find button for {option_name}")
                continue
            # Click button; this is a destructive method and removed obscuring elements
            selenium_wrapper.destroy_to_click(button[0])

            # Get dropdown list (this is not visible until button is clicked)
            drop_down_list = option_container.find_element(By.CLASS_NAME, "awsui-select-dropdown")
            # Select list element
            drop_down_list = drop_down_list.find_element(By.TAG_NAME, "ul")

            # Check if sub lists exist
            groups = drop_down_list.find_elements(By.TAG_NAME, "ul")
            if not groups:
                groups = [drop_down_list]

            for group in groups:
                for option in group.find_elements(By.TAG_NAME, "li"):
                    # Scrape options
                    try:
                        option_value = option.find_element(By.XPATH, "./div[@data-value]").get_attribute(
                            "data-value")
                        query_filters[option_name].append({
                            "name": option.text,
                            "data-value": option_value,
                        })
                    except selenium_exceptions.NoSuchElementException:
                        logger.warning(f"Unable to extract options for {option.text} {option.get_attribute('outerHTML')}")
                        continue

        return query_filters
