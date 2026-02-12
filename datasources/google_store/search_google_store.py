import re
import datetime
import requests
from requests.auth import HTTPBasicAuth

from common.lib.item_mapping import MappedItem
from common.lib.user_input import UserInput
from extensions.web_studies.datasources.apple_store.search_apple_store import SearchAppleStore, collect_from_store


class SearchGoogleStore(SearchAppleStore):
    """
    Search Google Store data source


    Defines methods to fetch data from Google's application store on demand
    """
    type = "google-store-search"  # job ID
    category = "Search"  # category
    title = "Google Play Store Search"  # title displayed in UI
    description = "Query Google's app store to retrieve data on applications and developers"  # description displayed in UI
    extension = "ndjson"  # extension of result file, used internally and in UI
    is_local = False  # Whether this datasource is locally scraped
    is_static = False  # Whether this datasource is still updated

    config = {
        "google-play-api.info": {
            "type": UserInput.OPTION_INFO, 
            "help": "Utilize Google Play API built by [Facundo Olano](https://github.com/facundoolano/google-play-api?tab=readme-ov-file)",
        },
        "google-play-api.enabled": {
            "type": UserInput.OPTION_TOGGLE,
            "help": "Enable Google Play API",
            "tooltip": "Connect to a third-party API to retrieve data from the Google Play Store via https://github.com/facundoolano/google-play-api.",
            "default": False,
            "global": True
        },
        "google-play-api.url": {
            "type": UserInput.OPTION_TEXT, 
            "help": "Google Play API URL", 
            "tooltip": "URL for the Google Play API e.g. http://localhost:3000/api/. Only used if 'Enable Google Play API' is toggled on.", 
            "default": "",
        },
        "google-play-api.basic-auth-user": {
            "type" : UserInput.OPTION_TEXT,
            "help": "Google Play API Basic Auth User", 
            "tooltip": "Optional authentication username sent to Google Play API.", 
            "default": "",
        },
        "google-play-api.basic-auth-pass": {
            "type" : UserInput.OPTION_TEXT,
            "help": "Google Play API Basic Auth Password", 
            "tooltip": "Optional authentication password sent to Google Play API.", 
            "default": "",
        }
    }

    @classmethod
    def get_options(cls, parent_dataset=None, config=None):
        google_api_enabled = config.get("google-play-api.enabled") and config.get("google-play-api.url")
        query_types = {
                    "query-app-detail": "App IDs",
                    "query-search-detail": "Search by query",
                    "query-developer-detail": "Developer IDs",
                    "query-similar-detail": "Similar Apps",
                    "query-permissions": "Permissions",
                    # NO list endpoint
                }
        if google_api_enabled:
            query_types["reviews"] = "App Reviews"

        options = {
            "intro-1": {
                "type": UserInput.OPTION_INFO,
                "help": ("This data source allows you to query [Google's app store](https://play.google.com/store/apps) to retrieve data on applications and developers."
                        "\nCountry options can be found [here](https://osf.io/gbjnu).")
            },
            "method": {
                "type": UserInput.OPTION_CHOICE,
                "help": "Query Type",
                "options": query_types,
                "default": "query-search-detail"
            },
            "query": {
                "type": UserInput.OPTION_TEXT_LARGE,
                "help": "List of App IDs, Developer IDs, or queries to search for.",
                "tooltip": "Seperate IDs or queries with commas to search multiple."
            },
            "full_details": {
                "type": UserInput.OPTION_TOGGLE,
                "help": "Include full application details",
                "default": False,
                "tooltip": "If enabled, the full details of each application will be included in the output.",
                "requires": "method$=detail", # ends with detail
            },
            "intro-2": {
                "type": UserInput.OPTION_INFO,
                "help": "Language and Country options have limited effects due to geographic restrictions and results given based on from what country the request originates (i.e. the country where 4CAT is based)."
            },
            "languages": {
                "type": UserInput.OPTION_TEXT,
                "help": "Languages to query.",
                "default": "en",
                "tooltip": "Seperate ISO two letter language codes with commas to search multiple languages. If left blank, only English will be used."
            },
            "countries": {
                "type": UserInput.OPTION_TEXT,
                "help": "Countries to query.",
                "default": "us",
                "tooltip": "Seperate ISO two letter country codes with commas to search multiple countries. If left blank, only US will be used.",
                "requires": "method!=reviews" # reviews endpoint does not support country parameter
            },
        }
        
        return options

    def get_items(self, query):
        """
        Fetch items from Google Store

        :param query:
        :return:
        """
        auth = None
        google_play_url = None
        if self.config.get("google-play-api.enabled"):
            if not self.config.get("google-play-api.url"):
                self.finish_with_error("Google Play API enabled, but no URL provided")
                return
            
            google_play_url = self.config.get("google-play-api.url")
            if self.config.get("google-play-api.basic-auth-user") and self.config.get("google-play-api.basic-auth-pass"):
                auth = HTTPBasicAuth(self.config.get("google-play-api.basic-auth-user"), self.config.get("google-play-api.basic-auth-pass"))

            # Test connection
            try:
                response = requests.get(google_play_url, auth=auth)
                if response.status_code != 200:
                    self.finish_with_error(f"Google Play API URL returned status code {response.status_code}")
                    return
            except requests.RequestException as e:
                self.finish_with_error(f"Error connecting to Google Play API: {e}")
                return

        # Prepare parameters for API or local scraping
        queries = [q.strip() for q in re.split(',|\n', self.parameters.get('query'))]
        countries = [c.strip() for c in re.split(',|\n', self.parameters.get('countries'))]
        languages = [l.strip() for l in re.split(',|\n', self.parameters.get('languages'))]
        if not countries or not languages or not queries:
            self.finish_with_error("At least one query, country, and language must be provided.") 
            return
        full_details = self.parameters.get('full_details', False)

        # Updated method from options to match the method names in the collect_from_store function
        method = self.option_to_method.get(self.parameters.get('method'))
        if method is None:
            self.log.warning(f"Google store unknown query method; check option_to_method dictionary matches method options.")
            self.finish_with_error("Unknown query method.")
            return

        if google_play_url:
            collector = "Google Play API"
            # Max number of results is 250
            params = {"num": 250}
            if method == 'search':
                # search requires ?q=
                api_endpoint = '/apps/'
                response_key = 'results'
            elif method == 'developer':
                api_endpoint = '/developers/{devId}'
                response_key = 'apps'
            elif method == 'similar':
                # similar requires /apps/APP_ID/similar
                api_endpoint = '/apps/{appId}/similar'
                response_key = 'results'
            elif method == 'permissions':
                # permissions requires /apps/APP_ID/permissions
                api_endpoint = '/apps/{appId}/permissions'
                response_key = 'results'
            elif method == 'app':
                api_endpoint = '/apps/{appId}'
                response_key = None
            elif method == 'reviews':
                api_endpoint = '/apps/{appId}/reviews'
                # reviews are nested under results.data in the API response
                # TODO: they also have pagination...
                response_key = 'results.data' 


            failed_queries = []
            no_results_queries = []
            num_results_collected = 0
            num_queries = len(queries) * len(countries) * len(languages)
            for query in queries:
                for country in countries:
                    for lang in languages:
                        self.dataset.log(f"Collecting {method} for '{query}' from Google Store for country '{country}' and language '{lang}'")
                        url = google_play_url.rstrip('/') + api_endpoint
                        if method == 'search':
                            params['q'] = query
                        elif method == 'developer':
                            url = url.replace('{devId}', query)
                        elif method in ['similar', 'permissions', 'app', 'reviews']:
                            url = url.replace('{appId}', query)
                        else:
                            self.dataset.log.warning(f"Unknown method '{method}' for Google Play API; check method handling in code.")
                            self.finish_with_error("Unknown query method.")
                            return
                        params['country'] = country
                        params['lang'] = lang
                        
                        try:
                            response = requests.get(url, params=params, auth=auth)
                            response.raise_for_status()
                            if response_key and '.' in response_key:
                                # Handle nested keys for reviews endpoint
                                keys = response_key.split('.')
                                results = response.json()
                                self.dataset.log(f"Parsing nested results for reviews endpoint with keys: {keys}")
                                self.dataset.log(f"Full response for reviews endpoint: {results}")
                                for key in keys:
                                    results = results.get(key, [])
                            else:
                                # For non-nested keys, just get the results list or wrap the single result in a list for consistency
                                results = response.json()[response_key] if response_key else [response.json()] 
                        except requests.RequestException as e:
                            self.dataset.log(f"Error fetching data from Google Play API for query '{query}', country '{country}', language '{lang}': {e}")
                            failed_queries.append((query, country, lang))

                        if results:
                            self.dataset.log(f"Collected {len(results)} results from Google Play Store for query '{query}', country '{country}', language '{lang}'")
                            if full_details and method in ['search', 'similar', 'developer']:
                                # If full details requested, get details for each app (API does not support full details in search/similar results)
                                for i, result in enumerate(results):
                                    app_id = result.get('appId')
                                    if app_id:
                                        detail_url = google_play_url.rstrip('/') + f"/apps/{app_id}"
                                        try:
                                            detail_response = requests.get(detail_url, params={'country': country, 'lang': lang}, auth=auth)
                                            detail_response.raise_for_status()
                                            detailed_result = detail_response.json()
                                            num_results_collected += 1
                                            yield {
                                                "collector": collector, 
                                                "query_method": method, 
                                                "full_details": full_details, 
                                                "collected_at_timestamp": datetime.datetime.now().timestamp(), 
                                                "query": query, 
                                                "country": country, 
                                                "lang": lang,
                                                "item_index": i,
                                                **detailed_result}
                                        except requests.RequestException as e:
                                            self.dataset.log(f"Error fetching details from Google Play API for app '{app_id}' in query '{query}', country '{country}', language '{lang}': {e}")
                                            failed_queries.append((f"{query} (app ID: {app_id})", country, lang))
                                    else:
                                        self.dataset.log(f"No app ID found in result for query '{query}', country '{country}', language '{lang}'; cannot fetch details.")
                                        failed_queries.append((f"{query} (no app ID)", country, lang))
                            else:
                                # If full details not requested or not supported for this method (permissions, app, reviews), yield results they are
                                yield from [{
                                    "collector": collector, 
                                    "query_method": method, 
                                    "full_details": full_details, 
                                    "collected_at_timestamp": datetime.datetime.now().timestamp(),
                                    "query": query, 
                                    "country": country, 
                                    "lang": lang,
                                    "item_index": i, 
                                    **result
                                    } for i, result in enumerate(results)]
                                num_results_collected += len(results)
                        else:
                            self.dataset.log(f"No results identified for '{query}' from Google Play Store for country '{country}' and language '{lang}'")
                            no_results_queries.append((query, country, lang))
            
            if failed_queries or no_results_queries:
                self.dataset.log("Summary of queries with issues:")
                if failed_queries:
                    self.dataset.log(f"Failed queries: {failed_queries}")
                if no_results_queries:
                    self.dataset.log(f"No results queries: {no_results_queries}")
                self.dataset.update_status(f"Completed {num_queries} queries and collected {num_results_collected} results w/ issues; see log for details", is_final=True)
            else:
                self.dataset.update_status(f"Completed {num_queries} queries successfully with {num_results_collected} total results", is_final=True)
        else:
            collector = "Google Play Scraper"
            # Legacy method using local scraping, which may be less reliable
            # Uses https://github.com/digitalmethodsinitiative/google-play-scraper which is based off of the above API; better to use the source maintained API
            params = {"num": 1000}  # Set a high limit to retrieve all results
            if method == 'search':
                params['queries'] = queries
            elif method == 'developer':
                params['devId'] = queries
            elif method == 'similar':
                params['appId'] = queries
            elif method == 'permissions':
                params['appId'] = queries
            else:
                params['appId'] = queries

            self.dataset.log(f"Collecting {method} from Google Store")
            results = collect_from_store('google', method, languages=languages, countries=countries, full_detail=full_details, params=params, log=self.dataset.log)
            if results:
                self.dataset.log(f"Collected {len(results)} results from Google Store")
                return [{"collector": collector,"query_method": method, "collected_at_timestamp": datetime.datetime.now().timestamp(), "item_index": i, **result} for i, result in enumerate(results)]
            else:
                self.dataset.log(
                    f"No results identified for {self.parameters.get('query', '') if method != 'lists' else self.parameters.get('collection')} from Google Play Store")
                return []

    @staticmethod
    def map_item(item):
        """
        Map item to a common format that includes, at minimum, "id", "thread_id", "author", "body", and "timestamp" fields.

        :param item:
        :return:
        """
        collector = item.pop("collector", "Google Play Scraper") # default to scraper if not specified
        query_method = item.pop("query_method", "")
        full_details = item.pop("full_details", False)
        query_term = item.pop("query", "")
        country = item.pop("country", "")
        formatted_item = {
            "4CAT_collector": item.pop("collector", ""),
            "4CAT_query_type": query_method,
            "query_term": query_term,
            "query_full_details": full_details,
            "query_language": item.pop("lang", ""),
        }
        if query_method != 'reviews': # reviews endpoint does not support country parameter
            formatted_item["query_country"] = country
        
        item_index = item.pop("item_index", "") # Used on query types without unique IDs (e.g., permissions)
        timestamp = item.get("collected_at_timestamp") # Used as fallback
        if collector == "Google Play API":
            if query_method == 'permissions': 
                # Permissions results do not have unique IDs, so we create a composite ID using the query term (app ID) and item index to ensure uniqueness
                formatted_item = {
                    "id": f"{query_term}_{item_index}", 
                    "body": item.get("permission", ""),
                    "type": item.get("type", "")
                }
            elif query_method == 'reviews':
                # Reviews
                # date is in "2026-02-11T15:33L:00.000Z" format
                date_reviewed = datetime.datetime.strptime(item.get("date"), "%Y-%m-%dT%H:%M:%S.%fZ") if item.get("date") else None
                formatted_item = {
                    "id": item["id"],
                    "date": date_reviewed.strftime('%Y-%m-%d %H:%M:%S') if date_reviewed else "",
                    "author": item.get("userName", ""),
                    "title": item.get("title", ""),
                    "body": item.get("text", ""),
                    "score": item.get("score", ""),
                    "review_link": item.get("url", ""),
                    "version": item.get("version", ""),
                    "thumbs_up": item.get("thumbsUp", ""),
                    "reply_text": item.get("replyText", ""),
                    "reply_date": datetime.datetime.strptime(item.get("replyDate"), "%Y-%m-%dT%H:%M:%S.%fZ").strftime('%Y-%m-%d %H:%M:%S') if item.get("replyDate") else "",
                    "user_image": item.get("userImage", ""),
                }
                timestamp = date_reviewed.timestamp() if date_reviewed else item.get("collected_at_timestamp")
            elif full_details or query_method == 'app':
                # All hit API /apps/APP_ID endpoint
                formatted_item = {
                    "id": item["appId"],
                    "title": item.get("title", ""),
                    "link": item.get("playstoreUrl"),
                    "developer_id": item.get("developerInternalID"),
                    "developer_name": item.get("developer", {}).get("devId"),
                    "author": item.get("developer", {}).get("devId"), # for compatibility with 4CAT expected fields
                    "developer_link": item.get("developerWebsite"),
                    "subject": item.get("summary"),
                    "body": item.get("description"),
                    "price_text": item.get("priceText"),
                    "price_currency": item.get("currency"),
                    "price": item.get("price"),
                    "in_app_purchases": item.get("offersIAP"),
                    "in_app_price_range": item.get("IAPRange"),
                    "available": item.get("available"),
                    "android_version": item.get("androidVersion"),
                    "privacy_policy_link": item.get("privacyPolicy"),
                    "genre": item.get("genre"),
                    "genre_id": item.get("genreId"),
                    "install_text": item.get("installs"),
                    "install_min": item.get("minInstalls"),
                    "install_max": item.get("maxInstalls"),
                    "score": item.get("score"),
                    "rating_count": item.get("ratings"),
                    "ratings_histogram": ", ".join([str(k)+": "+str(v) for k,v in item.get("histogram", {}).items()]),
                    "content_rating": item.get("contentRating"),
                    "content_description": item.get("contentRatingDescription"),
                    "ad_supported": item.get("adSupported"),
                    "released": item.get("released"),
                    "updated": item.get("updated"),
                    "version": item.get("version"),
                    "icon_link": item.get("icon"),
                    "header_link": item.get("headerImage"),
                    "screenshots": ", ".join(item.get("screenshots", [])),
                    }
                timestamp = item.get("updated") or item.get("released") or item.get("collected_at_timestamp")
                
            else:
                # search, similar, and developer endpoints return a subset of fields
                formatted_item = {
                    "id": item["appId"],
                    "title": item.get("title", ""),
                    "link": item.get("playstoreUrl"),
                    "developer_name": item.get("developer", {}).get("devId"),
                    "author": item.get("developer", {}).get("devId"), # for compatibility with 4CAT expected fields
                    "body": item.get("summary"),
                    "price_currency": item.get("currency"),
                    "price": item.get("price"),
                    "score": item.get("score"),
                    "icon_link": item.get("icon"),
                }
            
            # Released date in "Jun 2, 2023" format
            formatted_item["timestamp"] = datetime.datetime.strptime(item.get("released"), "%b %d, %Y").timestamp() if item.get("released") else timestamp
            formatted_item["collected_at_timestamp"] = item.get("collected_at_timestamp")

        else:
            # Google Play Scraper mapping - should match fields from the scraper, which differs from the API
            if query_method == 'app':
                formatted_item["id"] = item.get("id", "")
                body = item.get("description", "")
            elif query_method == 'list':
                formatted_item["id"] = item.get("id", "")
                body = item.get("description", "")
            elif query_method == 'search':
                formatted_item["query_term"] = item.pop("term", "")
                formatted_item["id"] = item.get("id", "")
                body = item.get("description", "")
            elif query_method == 'developer':
                formatted_item["id"] = item.get("id", "")
                body = item.get("description", "")
            elif query_method == 'similar':
                formatted_item["id"] = item.get("id", "")
                body = item.get("description", "")
            elif query_method == 'permissions':
                formatted_item["id"] = item_index
                body = item.get("permission", "")
            else:
                # Should not happen
                raise Exception("Unknown query method: {}".format(query_method))

            formatted_item["app_id"] = item.get("id", "")
            if "developer_link" in item:
                item["4cat_developer_id"] = item["developer_link"].split("dev?id=")[-1]
            # Map expected fields which may be missing and rename as desired
            mapped_fields = {
                "title": "title",
                "link": "link",
                "4cat_developer_id": "developer_id",
                "developer_name": "developer_name",
                "developer_link": "developer_link",
                "price_inapp": "price_inapp",
                "category": "category",
                "video_link": "video_link",
                "icon_link": "icon_link",
                "num_downloads_approx": "num_downloads_approx",
                "num_downloads": "num_downloads",
                "published_date": "published_date",
                "published_timestamp": "published_timestamp",
                "pegi": "pegi",
                "pegi_detail": "pegi_detail",
                "os": "os",
                "rating": "rating",
                "description": "description",
                "price": "price",
                "num_of_reviews": "num_of_reviews",
                "developer_email": "developer_email",
                "developer_address": "developer_address",
                "developer_website": "developer_website",
                "developer_privacy_policy_link": "developer_privacy_policy_link",
                "data_safety_list": "data_safety_list",
                "updated_on": "updated_on",
                "app_version": "app_version",
                "list_of_categories": "list_of_categories",
                "errors": "errors",
                "collected_at_timestamp": "collected_at_timestamp",
            }
            for field in mapped_fields:
                formatted_item[mapped_fields[field]] = item.get(field, "")

            # Add any additional fields to the item
            formatted_item["additional_data_in_ndjson"] = ", ".join(
                [f"{key}: {value}" for key, value in item.items() if key not in list(mapped_fields) +  ["app_id"]])

            # 4CAT required fields
            formatted_item["thread_id"] = ""
            formatted_item["author"] = item.get("developer_name", "")
            formatted_item["body"] = body
            # some queries do not return a publishing timestamp so we use the collected at timestamp
            try:
                timestamp = datetime.datetime.fromtimestamp(item.get("published_timestamp")).timestamp() if "published_timestamp" in item else datetime.datetime.strptime(item.get("published_date"), "%Y-%m-%d").timestamp() if "published_date" in item else item.get("collected_at_timestamp")
            except ValueError:
                timestamp = item.get("collected_at_timestamp")
            formatted_item["timestamp"] = int(timestamp)

        return MappedItem(formatted_item)