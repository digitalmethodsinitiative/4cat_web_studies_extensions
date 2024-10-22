from datetime import datetime
import urllib
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

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

    base_url = "https://cloud.google.com/products"

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
                "help": ("This data source allows you to query [Google Cloud's product store](https://cloud.google.com/products) to retrieve data on applications and developers."
                         )
            },
            "method": {
                "type": UserInput.OPTION_CHOICE,
                "help": "Query Type",
                "options": {
                    # "search": "Search",
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
            # "full_details": {
            #     "type": UserInput.OPTION_TOGGLE,
            #     "help": "Include full application details",
            #     "default": False,
            #     "tooltip": "If enabled, the full details of each application will be included in the output.",
            # },
        }
        categories = config.get("cache.google_cloud.categories", [])
        if categories:
            formatted_categories = {c: c for c in sorted(categories)}
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
            self.dataset.update_status("Selenium not available; unable to collect from AWS Store.", is_final=True)
            return

        method = self.parameters.get("method")
        if method == "categories":
            categories = self.parameters.get("categories", [])
            if not categories:
                raise ProcessorException("No category selected")
        else:
            raise ProcessorException("Invalid method")

        # using English as we are searching for components by their English names
        params = {"hl": "en"}
        url = self.base_url + ("?" + urllib.parse.urlencode(params)) if params else ""
        self.dataset.update_status(f"Fetching Google Cloud Store: {url}")
        success, errors = self.get_with_error_handling(url)
        if not success:
            self.dataset.log(f"Failed to fetch Google Cloud Store: {errors}")
            self.dataset.update_status("Unable to connect to Google Cloud Store.", is_final=True)
            return

        # Ensure page is loaded
        if not self.check_page_is_loaded():
            self.dataset.update_status("Google Cloud Store did not load after 60 seconds; try again later.", is_final=True)
            return
        self.scroll_down_page_to_load(60)

        collected = 0
        if method == "categories":
            # Check if categories are available
            try:
                existing_categories = GoogleCloudStoreCategories.get_category_filters(self.driver)
            except ProcessorException as e:
                self.dataset.log(self.driver.page_source)
                self.dataset.update_status(f"Failed to collect categories from Google Cloud Store: {e}", is_final=True)
                return
            if not existing_categories:
                self.dataset.update_status("Failed to find categories on Google Cloud Store; try again later.", is_final=True)
                return

            for category in categories:
                self.dataset.update_status(f"Processing category {category}")
                if self.interrupted:
                    raise ProcessorInterruptedException(f"Processor interrupted while fetching category {category}")

                # Find category
                if category not in existing_categories:
                    self.dataset.log(f"Category {category} not found on Google Cloud Store; skipping...")
                    continue

                category_xpath = f"//button/span[contains(text(), '{category}')]/.."
                WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH, category_xpath)))
                possible_buttons = self.driver.find_elements(By.XPATH, category_xpath)
                if not possible_buttons:
                    self.dataset.log(f"Failed to find category {category} on Google Cloud Store; skipping...")
                    continue

                # Click category
                category_button = possible_buttons[0]
                self.destroy_to_click(category_button) # cookies must be removed

                # Collect product search result blocks
                result_blocks_css_selector = ".x9K9hf.DDohKf"
                WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, result_blocks_css_selector)))
                collected_at = datetime.now()
                groups = self.driver.find_elements(By.CSS_SELECTOR, result_blocks_css_selector)
                if not groups:
                    self.dataset.log(f"Failed to find product groups for category {category} on Google Cloud Store; skipping...")
                    continue
                for i, group in enumerate(groups):
                    sub_category = group.find_element(By.CSS_SELECTOR, "h1").text
                    sub_category_description = group.find_element(By.CSS_SELECTOR, "span").text
                    results = group.find_elements(By.CSS_SELECTOR, "a")
                    if not results:
                        self.dataset.log(f"Failed to find products for sub-category {sub_category} on Google Cloud Store; skipping...")
                        continue

                    for j, result in enumerate(results):
                        yield {
                            "collected_at": collected_at.strftime("%Y-%m-%d %H:%M:%S"),
                            "category": category,
                            "sub_category": sub_category,
                            "sub_category_description": sub_category_description,
                            "sub_category_order": i+1,
                            "product_order": j+1,
                            "title": result.find_element(By.CSS_SELECTOR, "h1").text,
                            "subtitle": result.find_element(By.CSS_SELECTOR, "h2").text,
                            "link": result.get_attribute("href"),
                            "brief_description": result.find_element(By.XPATH, "./div/div").text,
                        }
                        collected += 1
                        self.dataset.update_status(f"Collected {collected} results")

                # Clear category selection


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
           
        return query


    @staticmethod
    def map_item(item):
        """
        Map item to a common format that includes, at minimum, "id", "thread_id", "author", "body", and "timestamp" fields.

        :param item:
        :return:
        """
        item["id"] = item["link"].replace("https://cloud.google.com/", "").replace("/", "_")
        item["body"] = item["brief_description"]
        return MappedItem(item)


class GoogleCloudStoreCategories(BasicWorker):
    """
    Collect Google Cloud Product Store categories and store them in database
    """
    type = "google-cloud-store-category-collector"  # job ID

    # Run every day to update categories
    ensure_job = {"remote_id": "google-cloud-store-category-collector", "interval": 86400}

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
        selenium_helper.selenium_log.info(f"Fetching category options from Google Cloud Store {categories_url}")

        selenium_helper.start_selenium()
        selenium_helper.driver.get(categories_url)
        if not selenium_helper.check_for_movement():
            raise ProcessorException("Failed to load Google Cloud Store")

        if not selenium_helper.check_page_is_loaded():
            raise ProcessorException("Google Cloud Store did not load and timed out")

        try:
            category_filters = self.get_category_filters(selenium_helper.driver)
            if category_filters:
                selenium_helper.selenium_log.info(f"Collected category options from Google Cloud Store: {category_filters}")
                config.set("cache.google_cloud.categories", category_filters)
                config.set("cache.google_cloud.categories_updated_at", datetime.now().timestamp())
            else:
                selenium_helper.selenium_log.warning("Failed to collect category options on Google Cloud Store")

        except ProcessorException as e:
            selenium_helper.selenium_log.error(f"Error collecting Google Cloud Store categories: {e}")
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
        possible_cat_boxes = driver.find_elements(By.XPATH,"//*[contains(text(), 'Browse by category')]/..")
        if not possible_cat_boxes:
            raise ProcessorException("Failed to find category options")

        cat_box = possible_cat_boxes[0]
        category_filters = []
        for button in cat_box.find_elements(By.CSS_SELECTOR, "button"):
            category_filters.append(button.text)

        return category_filters