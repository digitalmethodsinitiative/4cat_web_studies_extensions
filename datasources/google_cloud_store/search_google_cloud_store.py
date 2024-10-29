from datetime import datetime
import urllib
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.expected_conditions import staleness_of

from extensions.web_studies.selenium_scraper import SeleniumSearch, SeleniumWrapper
from backend.lib.worker import BasicWorker
from common.lib.exceptions import ProcessorInterruptedException, ProcessorException
from common.lib.item_mapping import MappedItem
from common.lib.user_input import UserInput
from common.config_manager import config

class SearchGoogleCloudStore(SeleniumSearch):
    """
    Search Google Cloud Product Store data source
    """
    type = "google-cloud-store-search"  # job ID
    category = "Search"  # category
    title = "Google Cloud Product Store Search"  # title displayed in UI
    description = "Query Google Cloud's product store to retrieve data on applications and developers"  # description displayed in UI
    extension = "ndjson"  # extension of result file, used internally and in UI
    is_local = False  # Whether this datasource is locally scraped
    is_static = False  # Whether this datasource is still updated

    base_url = "https://console.cloud.google.com/marketplace"

    # Categories are collected and cached
    config = {
        "cache.google_cloud.categories": {
            "type": UserInput.OPTION_TEXT_JSON,
            "help": "Google Cloud Product Categories",
            "tooltip": "automatically updated",
            "default": {}
        },
        "cache.google_cloud.categories_updated_at": {
            "type": UserInput.OPTION_TEXT,
            "help": "Google Cloud Product Categories Updated At",
            "tooltip": "automatically updated",
            "default": 0,
            "coerce_type": int
        }
    }

    @classmethod
    def get_options(cls, parent_dataset=None, user=None):
        max_results = 1000
        options = {
            "intro-1": {
                "type": UserInput.OPTION_INFO,
                "help": ("This data source allows you to query [Google Cloud's marketplace](https://console.cloud.google.com/marketplace) to retrieve data on applications and developers."
                         )
            },
            "method": {
                "type": UserInput.OPTION_CHOICE,
                "help": "Query Type",
                "options": {
                    "search": "Search",
                    "categories": "Categories",
                },
                "default": "categories"
            },
            "categories": {
                "type": UserInput.OPTION_TEXT,
                "help": "Categories",
                "requires": "method^=categories",  # starts with categories
            },
            "query": {
                "type": UserInput.OPTION_TEXT_LARGE,
                "help": "List of queries to search (leave blank for all).",
                "default": "", # need default else regex will fail
                "requires": "method^=search",  # starts with search
            },
            "amount": {
                "type": UserInput.OPTION_TEXT,
                "help": "Max number of results per category/query" + (f" (max {max_results:,})" if max_results != 0 else ""),
                "default": 40 if max_results == 0 else min(max_results, 40),
                "min": 0 if max_results == 0 else 1,
                "max": max_results,
                "tooltip": "The Google Cloud marketplace returns apps in batches of 40."
            },
            # "full_details": {
            #     "type": UserInput.OPTION_TOGGLE,
            #     "help": "Include full application details",
            #     "default": False,
            #     "tooltip": "If enabled, the full details of each application will be included in the output.",
            # },
        }
        categories = config.get("cache.google_cloud.categories", {})
        if categories:
            formatted_categories = {k: categories[k]["name"] for k in sorted(categories)}
            options["categories"]["options"] = formatted_categories
            options["categories"]["type"] = UserInput.OPTION_MULTI_SELECT
            options["categories"]["default"] = []
        else:
            options.pop("categories")
        
        return options

    def get_items(self, query):
        """
        Fetch items from Google Cloud Product Store

        :param query:
        :return:
        """
        if not self.is_selenium_available():
            self.dataset.update_status("Selenium not available; unable to collect from Google Cloud Marketplace.", is_final=True)
            return

        method = query.get("method")
        queries = query.get("query", []) + query.get("categories", [])
        max_results = self.parameters.get("amount", 40)

        # Identifiers depend on method
        if method == "categories":
            result_total_identifier = (By.CLASS_NAME, "cfc-shelf-header")
            result_blocks_identifier = (By.TAG_NAME, "cfc-result-card")
            title_identifier = (By.XPATH, ".//*[@role='heading']")
            sub_title_identifier = (By.CLASS_NAME, "cfc-result-card-subtitle")
            description_identifier = (By.CLASS_NAME, "cfc-result-card-description")
        elif method == "search":
            result_total_identifier = (By.TAG_NAME, "h1")
            result_blocks_identifier = (By.TAG_NAME, "mp-search-results-list-item")
            title_identifier = (By.TAG_NAME, "h3")
            sub_title_identifier = (By.TAG_NAME, "h4")
            description_identifier = (By.TAG_NAME, "p")
        else:
            raise ProcessorException("Invalid method")

        for query in queries:
            collected = 0
            if method == "categories":
                known_categories = self.config.get("cache.google_cloud.categories", {})
                current_category = known_categories.get(query, {})
                query = current_category.get("name")
                url = current_category.get("link")
            elif method == "search":
                url = f"{SearchGoogleCloudStore.base_url}/browse?q={urllib.parse.quote_plus(query)}"
            else:
                raise ProcessorException("Invalid method")

            self.dataset.update_status(f"Processing query {query}")

            success, errors = self.get_with_error_handling(url)
            if not success:
                self.dataset.log(f"Failed to fetch Google Cloud Store: {errors}")
                self.dataset.update_status("Unable to connect to Google Cloud Store.", is_final=True)
                return

            # Ensure page is loaded
            if not self.check_page_is_loaded():
                self.dataset.update_status("Google Cloud Store did not load after 60 seconds; try again later.",
                                           is_final=True)
                return
            self.scroll_down_page_to_load(60)
            collected_at = datetime.now()

            # Get total results
            try:
                WebDriverWait(self.driver, 5).until(EC.presence_of_element_located(result_total_identifier))
                results_header = self.driver.find_elements(*result_total_identifier)
            except TimeoutException:
                results_header = None
            if not results_header:
                # Unknown number results
                self.log.warning(f"Unable to parse Google Cloud results; page format may have changed")
                results_count = None
            else:
                results_count = results_header[0].text.replace(' results', '').replace(",", "")
                self.dataset.log(f"Found {results_count} total results for {query}")
                try:
                    results_count = int(results_count)
                except ValueError:
                    results_count = None

            # Collect product search result blocks
            WebDriverWait(self.driver, 5).until(EC.presence_of_element_located(result_blocks_identifier))
            results = self.driver.find_elements(*result_blocks_identifier)
            if not results:
                self.log.warning(f"Unable to parse results for query {query}")
                self.dataset.update_status("No results found", is_final=True)
                return

            while collected < max_results:
                for i, result in enumerate(results):
                    collected += 1
                    title_block = result.find_elements(*title_identifier)
                    product_link_block = result.find_elements(By.XPATH, ".//a")
                    sub_title_block = result.find_elements(*sub_title_identifier)
                    description_block = result.find_elements(*description_identifier)
                    thumb_block = result.find_elements(By.XPATH, ".//img")

                    if method == "categories":
                        type_block = result.find_elements(By.XPATH, ".//dt[contains(text(), 'Type ')]/../dd")
                        sub_title_text = sub_title_block[0].text if sub_title_block else None
                    elif method == "search":
                        if sub_title_block:
                            type_block = sub_title_block[0].find_elements(By.TAG_NAME, "span")
                            if type_block:
                                sub_title_text = sub_title_block[0].text.replace(type_block[0].text, "") if sub_title_block else None
                        else:
                            type_block = None
                            sub_title_text = None

                    yield {
                        "collected_at": collected_at.strftime("%Y-%m-%d %H:%M:%S"),
                        "query": query,
                        "rank": collected,
                        "title": title_block[0].text if title_block else None,
                        "subtitle": sub_title_text,
                        "link": product_link_block[0].get_attribute("href") if product_link_block else None,
                        "description": description_block[0].text if description_block else None,
                        "type": type_block[0].text if type_block else None,
                        "thumbnail": thumb_block[0].get_attribute("src") if thumb_block else None,
                        "html": result.get_attribute("outerHTML")
                    }
                    self.dataset.update_status(f"Collected {collected} results")

                if collected >= max_results or (results_count and collected >= results_count):
                    break

                # Check if there are more results
                # Note: could also use "page=" in URL though not actual URL query param
                next_button = self.driver.find_elements(By.CLASS_NAME, "cfc-table-pagination-nav-button-next")
                if not next_button:
                    self.log.warning(f"Google Cloud page may have changed; unable to find next button")
                    self.dataset.update_status(f"Unable to continue to next page for query {query}")
                    break
                # Click next button
                next_button[0].click()
                # Ensure old results are gone
                WebDriverWait(self.driver, 5).until(staleness_of(results[0]))
                # Wait for new results
                WebDriverWait(self.driver, 5).until(EC.presence_of_element_located(result_blocks_identifier))
                # Update collected time
                collected_at = datetime.now()
                results = self.driver.find_elements(*result_blocks_identifier)
                if not results:
                    self.log.warning(f"Unable to parse results for query {query}")
                    self.dataset.update_status(f"Unable to continue to next page for query {query}")
                    break

    def get_app_details(self, app):
        """
        Collect full details for an app
        """
        pass


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
        method = query.get("method")
        categories = []
        queries = []
        if method == "categories":
            categories = query.get("categories", [])
            if not categories:
                raise QueryParametersException("No category selected")
        elif method == "search":
            queries = query.get("query", "")
            if not queries.strip():
                raise QueryParametersException("No search query provided")

            queries = queries.replace("\n", ",").split(",")
        else:
            raise ProcessorException("Invalid method")
           
        return {
            "method": method,
            "categories": categories,
            "query": queries,
            "amount": int(query.get("amount", 40)),
        }


    @staticmethod
    def map_item(item):
        """
        Map item to a common format that includes, at minimum, "id", "thread_id", "author", "body", and "timestamp" fields.

        :param item:
        :return:
        """
        item["id"] = item["link"].replace("https://console.cloud.google.com/marketplace/product/", "").replace("/", "_")
        item["body"] = item["description"]
        item["timestamp"] = item["collected_at"]
        # Removing HTML; can be accessed via original item if desired
        item.pop("html")
        return MappedItem(item)


class GoogleCloudStoreCategories(BasicWorker):
    """
    Collect Google Cloud Product Store categories and store them in database
    """
    type = "google-cloud-store-category-collector"  # job ID

    # Run every day to update categories
    ensure_job = {"remote_id": "google-cloud-store-category-collector", "interval": 86400}

    cat_filter_xpath = "//cfc-unfold[.//span[contains(text(), 'Category')]]"

    def work(self):
        """
        Collect Google Cloud Product Store categories and store them in database via Selenium
        """
        # using English as we are searching for components by their English names
        params = {"hl": "en"}
        categories_url = SearchGoogleCloudStore.base_url + ("?" + urllib.parse.urlencode(params)) if params else ""
        selenium_helper = SeleniumWrapper()
        if not selenium_helper.is_selenium_available():
            raise ProcessorException("Selenium is not available; cannot collect categories from Google Cloud Store")

        # Backend runs get_options for each processor on init; but does not seem to have logging
        selenium_helper.selenium_log.info(f"Fetching category options from Google Cloud Marketplace {categories_url}")

        selenium_helper.start_selenium()
        selenium_helper.driver.get(categories_url)
        if not selenium_helper.check_for_movement():
            raise ProcessorException("Failed to load Google Cloud Marketplace")

        if not selenium_helper.check_page_is_loaded():
            raise ProcessorException("Google Cloud Marketplace did not load and timed out")

        selenium_helper.scroll_down_page_to_load(60)

        try:
            category_filters = self.get_category_filters(selenium_helper.driver)
            if category_filters:
                self.log.info(f"Collected category options ({len(category_filters)}) from Google Cloud Marketplace")
                config.set("cache.google_cloud.categories", category_filters)
                config.set("cache.google_cloud.categories_updated_at", datetime.now().timestamp())
            else:
                self.log.warning("Failed to collect category options from Google Cloud Marketplace")

        except ProcessorException as e:
            self.log.error(f"Error collecting Google Cloud Store categories: {e}")
        finally:
            # Always quit selenium
            selenium_helper.quit_selenium()


        return

    @staticmethod
    def get_category_filters(driver):
        """
        Get category filters from Google Cloud Store
        """
        # Get Category options
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, GoogleCloudStoreCategories.cat_filter_xpath)))
        possible_cat_boxes = driver.find_elements(By.XPATH, GoogleCloudStoreCategories.cat_filter_xpath)
        if not possible_cat_boxes:
            raise ProcessorException("Failed to find category options")

        cat_box = possible_cat_boxes[0]
        category_filters = {}
        for link in cat_box.find_elements(By.CSS_SELECTOR, "a"):
            cat_name = link.text.split("\n")[0] # remove extra text (i.e., (num of items))
            if cat_name:
                category_filters[cat_name.replace(" ", "_").lower()] = {
                    "name": cat_name,
                    "link": link.get_attribute("href")
                }

        return category_filters