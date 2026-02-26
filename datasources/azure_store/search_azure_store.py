import datetime
import re
import json
import urllib
from bs4 import BeautifulSoup

from backend.lib.worker import BasicWorker
from common.lib.exceptions import ProcessorInterruptedException, ProcessorException
from common.lib.item_mapping import MappedItem
from common.lib.user_input import UserInput
from common.lib.helpers import url_to_hash
from extensions.web_studies.selenium_scraper import SeleniumWrapper, SeleniumSearch


class SearchAzureStore(SeleniumSearch):
    """
    Search Microsoft Azure Store data source
    """
    type = "azure-store-search"  # job ID
    category = "Search"  # category
    title = "Microsoft Azure Store Search"  # title displayed in UI
    description = "Query Microsoft's Azure app store to retrieve data on applications and developers"  # description displayed in UI
    extension = "ndjson"  # extension of result file, used internally and in UI
    is_local = False  # Whether this datasource is locally scraped
    is_static = False  # Whether this datasource is still updated

    base_url = "https://azuremarketplace.microsoft.com"

    config = {
        "cache.azure.categories": {
            "type": UserInput.OPTION_TEXT_JSON,
            "help": "Azure Categories",
            "tooltip": "automatically updated",
            "default": {},
            "indirect": True
        },
        "cache.azure.categories_updated_at": {
            "type": UserInput.OPTION_TEXT,
            "help": "Azure Categories Updated At",
            "tooltip": "automatically updated",
            "default": 0,
            "coerce_type": float,
            "indirect": True
        }
    }

    @classmethod
    def is_compatible_with(cls, module=None, config=None):
        """
        Allow if Selenium is available
        """
        return SeleniumSearch.is_selenium_available(config=config)

    @classmethod
    def get_options(cls, parent_dataset=None, config=None):
        max_results = 1000
        options = {
            "intro-1": {
                "type": UserInput.OPTION_INFO,
                "help": ("This data source allows you to query [Microsoft's Azure app store](https://azuremarketplace.microsoft.com) to retrieve data on applications and developers."
                         )
            },
            "amount": {
                "type": UserInput.OPTION_TEXT,
                "help": "Max number of results per query" + (f" (max {max_results:,})" if max_results != 0 else ""),
                "default": 60 if max_results == 0 else min(max_results, 60),
                "min": 0 if max_results == 0 else 1,
                "max": max_results,
                "tooltip": "The Azure store returns apps in batches of 60."
            },
            "method": {
                "type": UserInput.OPTION_CHOICE,
                "help": "Query Type",
                "options": {
                    "search": "Search",
                    "category": "Category",
                },
                "default": "search"
            },
            "category": {
                "type": UserInput.OPTION_TEXT,
                "help": "Apple Store App Collection",
                "requires": "method^=category",  # starts with list
            },
            "query": {
                "type": UserInput.OPTION_TEXT_LARGE,
                "help": "List of queries to search (leave blank for all).",
                "default": "", # need default else regex will fail
            },
            "full_details": {
                "type": UserInput.OPTION_TOGGLE,
                "help": "Include full application details",
                "default": False,
                "tooltip": "If enabled, the full details of each application will be included in the output.",
            },
        }
        categories = config.get("cache.azure.categories", default={})
        if categories:
            formatted_categories = {f"{key}": f"{cat.get('cat_title')} - {cat.get('sub_title')}" for key, cat in
                                    categories.items()}
            formatted_categories["4CAT_all_categories"] = "All Categories"
            options["category"]["options"] = formatted_categories
            options["category"]["type"] = UserInput.OPTION_CHOICE
            options["category"]["default"] = "4CAT_all_categories"
        else:
            options.pop("category")
        
        return options

    def get_items(self, query):
        """
        Fetch items from Azure Store

        :param query:
        :return:
        """
        if not self.is_selenium_available(config=self.config):
            self.dataset.update_status("Selenium not available; unable to collect from Azure Store.", is_final=True)
            return

        queries = query.get("query", [""])
        if not queries:
            # can search all
            queries = [""]
        max_results = int(query.get('amount', 60))
        full_details = query.get('full_details', False)
        main_category = None
        sub_category = None

        if query.get('category'):
            category = query.get('category')
            if category == "4CAT_all_categories":
                # default app URL is used for all categories
                pass
            else:
                if "_--_" in category:
                    main_category, sub_category = category.split("_--_", 1)
                elif "_" in category:
                    # Backwards compatibility with old cached keys
                    main_category, sub_category = category.split("_", 1)

        count = 0
        for query in queries:
            self.dataset.update_status(f"Processing query {query}")
            if self.interrupted:
                raise ProcessorInterruptedException(f"Processor interrupted while fetching query {query}")
            page = 1
            query_results = 0
            while True:
                results = self.get_query_results(query, category=main_category, sub_category=sub_category, previous_results=query_results, page=page)
                if not results:
                    self.dataset.update_status(f"No additional results found for query {query}")
                    break

                for result in results:
                    if full_details:
                        if self.interrupted:
                            # Only interrupting if we are collecting full details as otherwise we have already collected everything
                            raise ProcessorInterruptedException(f"Processor interrupted while fetching details for {result.get('title')}")

                        if query_results >= max_results:
                            break
                        result = self.get_app_details(result)

                    result["id"] = url_to_hash(self.base_url + result.get("href")) # use URL as unique ID
                    result["4CAT_metadata"] = {"query": query, "category": main_category if main_category is not None else "all", "sub_category": sub_category, "page": page, "full_details_collected": full_details, "collected_at_timestamp": datetime.datetime.now().timestamp()}
                    yield result
                    count += 1
                    query_results += 1

                    self.dataset.update_status(f"Processed {query_results}{' of ' + str(max_results) if max_results > 0 else ''} for query {query}")
                    if max_results > 0:
                        self.dataset.update_progress(query_results / (max_results * len(queries)))

                if query_results >= max_results:
                    # We may have extra result as results are batched
                    break

                page += 1

    def get_app_details(self, app):
        """
        Collect full details for an app
        """
        app_url = self.base_url + app["href"]
        success, errors = self.get_with_error_handling(app_url)
        if not success:
            self.dataset.log(f"Failed to fetch details for app {app.get('title')} from Azure Store: {errors}")
            return app

        if not self.check_page_is_loaded():
            self.dataset.log(f"Timed out loading details for app {app.get('title')} from Azure Store")
            return app

        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        # General content
        # ID
        id_block = soup.find_all(attrs={"data-bi-name":True})
        app["item_id"] = id_block[0].get("data-bi-name") if id_block else None
        
        # Title block
        title_block = soup.find("div", attrs={"class": "titleBlock"})
        app["full_title"] = title_block.find("h1").get_text()
        app["developer_name"] = title_block.find("h2").get_text()
        
        # Icon (there is a JSON we might be interested in extracting)
        icon_block = str(soup).split("\"iconURL\":\"")
        if not icon_block:
            app["icon_link"] = ""
        else:
            app["icon_link"] = icon_block[1].split("\"")[0]

        # Metadata
        metadata = {}
        for metadata_item in soup.find_all("meta"):
            metadata[metadata_item.get("itemprop")] = metadata_item.get("content")
        
        app["rating"] = metadata.get("ratingValue")
        app["review_count"] = metadata.get("reviewCount")
        
         # Badges
        badges_block = soup.find("div", attrs={"class": "ms-Stack-inner"})
        app["badges"] = []
        for block in badges_block.find_all("a"):
            app["badges"].append({
                "name": block.get("title"),
                "link": block.get("href"),
            })
        
        # Overview
        selected_tab = soup.find(attrs={"class": "tabSelected"}).get_text().lower()
        if selected_tab != "overview":
            raise ProcessorException(f"Unexpected selected tab when fetching app details from Azure Store: {selected_tab}")
        tab_content = soup.find_all("div", attrs={"class": "tabContent"})
        app["overview"] = tab_content[0].get_text(separator="\n") if tab_content else ""
        
        app ["metadata"] = metadata

        json_data = self.parse_azure_json(soup)
        app_position = json_data.get("apps", {}).get("idMap", {}).get(app["item_id"]) if json_data else None
    
        app["json_data"] = json_data.get("apps").get("dataList")[app_position] if (json_data and app_position is not None) else {}
        
        return app

    def get_query_results(self, query, category=None, sub_category=None, previous_results=0, page=1, store="en-us"):
        """
        Fetch query results from Azure Store
        """
        query_url = self.base_url + f"/{store}/marketplace/apps"
        if category:
            query_url += f"/category/{category}"
        params = {
            "page": page
        }
        if query:
            params["search"] = query

        if sub_category:
            params["subcategories"] = sub_category

        if params:
            query_url += "?" + urllib.parse.urlencode(params)

        success, errors = self.get_with_error_handling(query_url)
        if not success:
            raise ProcessorException(f"Failed to fetch data from Azure Store: {errors}")

        if not self.check_page_is_loaded():
            raise ProcessorException("Azure Store did not load and timed out")

        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        results = soup.find_all(attrs={"class": "tileContainer"})

        return [{
            "title": soup.find(attrs={"class": "title"}).get_text(),
            "href": soup.get("href"),
            "rank": i+previous_results,
            "source": str(soup),
            } for i, soup in enumerate(results, start=1)]

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
        queries = [q.strip() for q in re.split(',|\n', query.get('query', "")) if q.strip()]
        if not queries:
            queries = [""]

        return {
            "method": query.get("method", "search"),
            "query": queries,
            "category": query.get("category", "4CAT_all_categories"),
            "amount": query.get('amount', 60),
            "full_details": query.get("full_details", False),
        }


    @staticmethod
    def map_item(item):
        """
        Map item to a common format that includes, at minimum, "id", "thread_id", "author", "body", and "timestamp" fields.

        :param item:
        :return:
        """
        collected_at = datetime.datetime.fromtimestamp(item.get("4CAT_metadata", {}).get("collected_at_timestamp", ""))
        if type(collected_at) is int:
            collected_at = datetime.datetime.fromtimestamp(collected_at)
            collected_at.strftime("%Y-%m-%d %H:%M:%S")
        else:
            try:
                collected_at = collected_at.strftime("%Y-%m-%d %H:%M:%S")
                collected_at.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                collected_at = str(collected_at)
        
        formatted_item = {
            "id": item.get("item_id"),
            "query": item.get("4CAT_metadata", {}).get("query", ""),
            "category": item.get("4CAT_metadata", {}).get("category", ""),
            "sub_category": item.get("4CAT_metadata", {}).get("sub_category", ""),
            "collected_at": collected_at,
            "rank": item.get("rank"),
            "title": item.get("title", ""),
            "developer_name": item.get("developer_name", ""),
            "icon_link": item.get("icon_link", ""),
            "url": SearchAzureStore.base_url + item.get("href", ""),
            "full_details_collected": item.get("4CAT_metadata", {}).get("full_details_collected", False),
            "full_title": item.get("full_title", ""),
            "overview": item.get("overview"),
            "rating": item.get("rating", ""),
            "review_count": item.get("review_count", ""),
            "badges": ", ".join([badge.get("name") for badge in item.get("badges", [])]),

            # pricing info KEY appears in app json, but need to parse larger json for details
            # "pricing_information": ", ".join([detail.get("text") for detail in item.get("details", {}).get("pricing information", [])]),
            "categories": ", ".join([cat.get("longTitle") for cat in item.get("json_data", {}).get("categoriesDetails", []) if cat.get("longTitle")]),
            "support": item.get("json_data", {}).get("detailInformation", {}).get('SupportLink'),
            "privacy_policy": item.get("json_data", {}).get("detailInformation", {}).get('PrivacyPolicyUrl'),
            "license_terms": item.get("json_data", {}).get("licenseTermsUrl"),
            
            # 4CAT standard fields
            "body": item.get("overview"),
            "timestamp": int(item.get("4CAT_metadata", {}).get("collected_at_timestamp")),
        }

        return MappedItem(formatted_item)

    @staticmethod
    def parse_azure_json(soup, log=None, debug=False):
        """
        Parse JSON object from Azure Store.

        Pass `debug=True` and `log=<logger>` to enable detailed diagnostics.
        """
        scripts = soup.find_all("script")
        if debug and log:
            log.info(f"Parsing JSON data from Azure Store, found {len(scripts)} script tags to check")

        decoder = json.JSONDecoder()
        markers = [
            ("window.__INITIAL_STATE__", r"window\.__INITIAL_STATE__\s*="),
            ("window.INITIAL_STATE", r"window\.INITIAL_STATE\s*="),
        ]

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
                    log.info(f"Successfully parsed JSON data from Azure Store using {marker_used} in Script[{i}]")
                return parsed
            except json.JSONDecodeError as e:
                if debug and log:
                    log.warning(f"Failed JSON decode for Script[{i}] with {marker_used}: {e}")
                continue

        if debug and log:
            log.error("! Failed to find JSON data from Azure Store")
        return None


class AzureCategories(BasicWorker):
    """
    Collect Azure Store categories and store them in database
    """
    type = "azure-store-category-collector"  # job ID
    job_interval = 86400

    @classmethod
    def ensure_job(cls, config=None):
        """
        Ensure job is scheduled to run every day
        """
        # Run every day to update categories
        if "azure-store" in config.get("datasources.enabled"):
            return {"remote_id": "azure-store-category-collector", "interval": cls.job_interval}
        return None
    
    def work(self):
        """
        Collect Azure Store categories and store them in database via Selenium
        """
        categories_url = SearchAzureStore.base_url + "/en-us/marketplace/apps"
        selenium_wrapper = SeleniumWrapper()
        if not selenium_wrapper.is_selenium_available(config=self.config):
            raise ProcessorException("Selenium is not available; cannot collect categories from Azure Store")

        selenium_wrapper.start_selenium(config=self.config)
        try:
            selenium_wrapper.selenium_log.info(f"Fetching category options from Azure Store {categories_url}")
            selenium_wrapper.driver.get(categories_url)
            if not selenium_wrapper.check_for_movement():
                raise ProcessorException("Failed to load Azure Store")

            if not selenium_wrapper.check_page_is_loaded():
                raise ProcessorException("Azure Store did not load and timed out")

            soup = BeautifulSoup(selenium_wrapper.driver.page_source, "html.parser")

            # Only main categories are loaded in HTML; we can extract more from a JSON object
            json_data = SearchAzureStore.parse_azure_json(soup)
            if not json_data:
                self.log.error(f"Failed to parse categories from Azure Store JSON (retrying in {self.job_interval})")
                return

            category_map = {}
            categories = json_data.get("apps", {}).get("dataMap", {}).get("categories", {})
            # We need both the main and sub categories keys
            for _, cat in categories.items():
                main_key = cat.get("UrlKey")
                cat_title = cat.get("LongTitle")
                sub_cats = cat.get("SubCategoryDataMapping", {})
                if not main_key or not cat_title:
                    continue
                for _, sub_cat in sub_cats.items():
                    sub_title = sub_cat.get("LongTitle")
                    sub_key = sub_cat.get("UrlKey")
                    if not sub_key or not sub_title:
                        continue
                    category_map[main_key + "_--_" + sub_key] = {
                        "cat_key": main_key,
                        "cat_title": cat_title,
                        "sub_key": sub_key,
                        "sub_title": sub_title,
                    }

            if category_map:
                self.log.info(
                    f"Collected category options ({len(category_map)}) from Azure Store"
                )
                self.config.set("cache.azure.categories", category_map)
                self.config.set("cache.azure.categories_updated_at", datetime.datetime.now().timestamp())
            else:
                self.log.warning("Failed to collect category options from Azure Store")
        except ProcessorException as e:
            self.log.error(f"Error collecting Azure Store categories: {e}")
        finally:
            selenium_wrapper.quit_selenium()
