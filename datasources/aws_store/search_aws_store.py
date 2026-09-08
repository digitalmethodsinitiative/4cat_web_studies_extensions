"""
Amazon Web Services (AWS) Marketplace data source.

Until 2026-09 this data source drove a headless Firefox through
aws.amazon.com/marketplace/search/ with Selenium and extracted results.

It now calls the JSON service that page uses:
    service  com.amazonaws.marketplace.discovery.api.v20200320
    see      aws_marketplace_api.AwsMarketplaceApi

The service is undocumented and unsupported, but needs no browser, no
credentials and no cookies; it paginates 100 results per request instead of 20
and it returns more per listing than the page displayed, including the full
long description, the complete category and badge lists, and review counts and
ratings.

Because the endpoint is undocumented, responses are checked against a shallow
contract (see the API client) and the fields this module treats as guaranteed
are read by subscript rather than `.get()`, so that a change in AWS's response
shape raises a MapItemException and reaches an operator through 4CAT's
unmappable-item alerting instead of quietly producing empty columns. Fields that
AWS legitimately omits per listing are marked MissingMappedField, so a reader can
tell "AWS did not publish this" apart from "this is empty".

Known limitations
-----------------
- `ai_listing_highlights` was generated in the web UI and is not available from
  the API. The column is kept for continuity with datasets collected before the
  switch, and is always marked missing on newly collected items.
- `timestamp` is the collection time, not a listing publication date. AWS does
  not expose the latter.
"""

from collections import Counter
from datetime import datetime
import itertools
import re

from extensions.web_studies.datasources.aws_store.aws_marketplace_api import (
    AwsMarketplaceApi,
    AwsMarketplaceApiError,
    AwsMarketplaceContractError,
)
from backend.lib.search import Search
from backend.lib.worker import BasicWorker
from common.lib.exceptions import ProcessorInterruptedException, QueryNeedsExplicitConfirmationException
from common.lib.helpers import add_notification
from common.lib.item_mapping import MappedItem, MissingMappedField
from common.lib.user_input import UserInput


class SearchAwsStore(Search):
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
    listing_url = base_url + "pp/"
    vendor_url = base_url + "seller-profile?id="

    # The API paginates without an upper bound we could find (13,000 results
    # collected in one run during testing, still handing out page tokens)
    max_results = 10000

    # 4CAT filter name -> (Discovery API facet/filter type, label for "no filter")
    # The facet type doubles as the Filters[].Type accepted by SearchListings.
    filter_facets = {
        "Categories": ("CATEGORY", "All categories"),
        "Vendors": ("CREATOR", "All vendors"),
        "Pricing Models": ("PRICING_MODEL", "All pricing models"),
        "Delivery Methods": ("FULFILLMENT_OPTION_TYPE", "All delivery methods"),
    }

    # 4CAT filter name -> dataset parameter name
    query_param_map = {
        "Categories": "category",
        "Vendors": "creator",
        "Pricing Models": "pricing_model",
        "Delivery Methods": "fulfillment_option_type",
    }

    # Sentinel option names that mean "do not filter on this"
    query_param_ignore = [label for _, label in filter_facets.values()]

    # Warn in the interface when the cached filter vocabulary is older than this
    stale_options_age = 7 * 86400

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
                "tooltip": "The AWS Marketplace API returns apps in batches of 100."
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

        # Query options collected every day by the AwsStoreCategories worker below
        cached_options = config.get("cache.aws.query_options", {})
        for filter_name, filter_options in cached_options.items():
            if filter_name not in cls.query_param_map:
                config.db.log.warning(f"AWS Unknown filter name: {filter_name}")
                continue

            if not filter_options:
                # Some filters may not have been collected due to API changes
                continue

            options[cls.query_param_map[filter_name]] = {
                "type": UserInput.OPTION_MULTI_SELECT,
                "help": f"Filter by {filter_name}",
                "options": {(option["name"] if option["name"] not in cls.query_param_ignore else "all"): option["name"] for option in filter_options},
                "default": "all",
                "requires": "advanced_filters==true",
            }

        # The filter list is refreshed daily; if that has not happened, say so
        # rather than presenting a stale vocabulary as current.
        updated_at = config.get("cache.aws.query_options_updated_at", 0)
        if cached_options and updated_at:
            age = datetime.now().timestamp() - updated_at
            if age > cls.stale_options_age:
                options["intro-1"]["help"] += (
                    f"\n\n**Note:** the filter options below were last refreshed "
                    f"{int(age // 86400)} days ago and may be out of date."
                )

        return options

    def get_items(self, query):
        """
        Fetch items from the AWS Marketplace Discovery API.

        Each combination of query and filter values is collected as its own
        search, and every result records which combination produced it. That is
        more requests than folding the filters into one call would be -- the API
        accepts several values per filter -- but it keeps per-result provenance,
        which is what makes a filtered dataset interpretable after the fact.

        :param dict query:  Dataset parameters
        :return generator:
        """
        api = AwsMarketplaceApi(logger=self.log)

        search_queries = query.get("search_query", [])
        if not search_queries:
            # an empty query collects everything
            search_queries = [""]
        max_results = int(query.get("amount", 60))

        # Resolve the display names stored in the dataset parameters back to the
        # codes the API expects, using the vocabulary the daily worker cached.
        filter_options = {}
        all_filters = self.config.get("cache.aws.query_options", {})
        for filter_name, filters in all_filters.items():
            if filter_name not in self.query_param_map:
                self.log.warning(f"AWS Unknown filter name ({self.dataset.key}): {filter_name}")
                continue
            filter_options[self.query_param_map[filter_name]] = {
                option["name"]: option["data-value"]
                for option in filters if option["name"] not in self.query_param_ignore
            }

        # Record which vocabulary this dataset was collected against; filter
        # codes change over time and a dataset should say which set it used.
        updated_at = self.config.get("cache.aws.query_options_updated_at", 0)
        if updated_at:
            self.dataset.log(f"Using AWS filter options last refreshed {datetime.fromtimestamp(updated_at).strftime('%Y-%m-%d %H:%M')}")
        else:
            self.dataset.log("AWS filter options have never been refreshed; only unfiltered queries will work")

        # parameter name -> API filter type, in the order combinations are built
        parameter_facets = {
            self.query_param_map[filter_name]: facet_type
            for filter_name, (facet_type, _) in self.filter_facets.items()
        }
        parameter_names = list(parameter_facets.keys())
        parameter_values = [query.get(name, ["all"]) for name in parameter_names]

        combinations = list(itertools.product(search_queries, *parameter_values))
        total_queries = len(combinations)
        self.dataset.update_status(f"Collecting {total_queries} queries (max {max_results} per query) from AWS Store")

        missing_filters = []
        contract_failure = None
        tried_queries = 0

        for combination in combinations:
            if self.interrupted:
                raise ProcessorInterruptedException("Interrupted while collecting AWS Store queries")

            tried_queries += 1
            search_query = combination[0]
            chosen = dict(zip(parameter_names, combination[1:]))

            filters, filter_codes, unresolved = self.build_filters(chosen, parameter_facets, filter_options)
            if unresolved:
                for name in unresolved:
                    if name not in missing_filters:
                        missing_filters.append(name)
                    self.dataset.update_status(f"Filter code not found for {name}")
                self.dataset.update_progress(tried_queries / total_queries)
                continue

            collected = 0
            total_available = None
            try:
                for listing, total_available, page in api.search_listings(
                        search_text=search_query if search_query else None,
                        filters=filters if filters else None,
                        max_results=max_results,
                        interrupt_check=self.raise_if_interrupted):
                    collected += 1
                    yield {
                        "id": listing.get("Id"),
                        "listing": listing,
                        "4CAT_metadata": {
                            "collection_method": "discovery-api",
                            "api_version": api.api_version,
                            "query": search_query,
                            **chosen,
                            # The codes used to filter results in the AWS Store
                            "filter_codes": filter_codes,
                            "page": page,
                            "rank": collected,
                            "collected_at_timestamp": datetime.now().timestamp(),
                        },
                    }
            except AwsMarketplaceContractError as e:
                # The API no longer looks the way this data source expects. That
                # affects every remaining query equally, so stop rather than
                # grind through them; what has been collected is still written.
                contract_failure = str(e)
                self.log.error(f"AWS Marketplace API contract failure ({self.dataset.key}): {e}")
                self.dataset.log(f"AWS Marketplace API returned an unexpected response: {e}")
                break
            except AwsMarketplaceApiError as e:
                self.dataset.log(f"Unable to collect AWS results for query {search_query!r}: {e}")
                self.dataset.update_progress(tried_queries / total_queries)
                continue

            if collected == 0:
                self.dataset.log(f"No results for query {search_query!r} with filters {filter_codes}")

            self.dataset.update_progress(tried_queries / total_queries)
            self.dataset.update_status(
                f"Collected {collected}{f' of {total_available}' if total_available is not None else ''} results for "
                f"query: {search_query if search_query else 'no query provided'}, "
                + ", ".join(f"{name}: {value}" for name, value in chosen.items())
                + f" ({tried_queries} of {total_queries})"
            )

        if contract_failure:
            add_notification(
                self.db, "!admin",
                f"The AWS Marketplace API returned an unexpected response and collection was stopped early "
                f"(dataset `{self.dataset.key}`). This usually means AWS changed the API. Details: {contract_failure}"
            )
            self.dataset.update_status(
                "AWS Marketplace returned an unexpected response; collection stopped early and this dataset is "
                "incomplete. See the dataset log for details.", is_final=True)
        elif missing_filters:
            self.dataset.log(f"Missing filter codes needed to search for: {', '.join(missing_filters)}")
            self.dataset.update_status("Not all filters could be found; see log for details", is_final=True)

    def raise_if_interrupted(self):
        """
        Abort a long crawl promptly when 4CAT asks the worker to stop.

        Passed to the API client so pagination can be broken between pages
        rather than only between queries.
        """
        if self.interrupted:
            raise ProcessorInterruptedException("Interrupted while collecting AWS Store results")

    @staticmethod
    def build_filters(chosen, parameter_facets, filter_options):
        """
        Translate chosen filter display names into API filter clauses.

        :param dict chosen:  Parameter name -> chosen display name (or "all")
        :param dict parameter_facets:  Parameter name -> API filter type
        :param dict filter_options:  Parameter name -> {display name: code}
        :return tuple:  (filters, filter_codes, unresolved names)
        """
        filters = []
        filter_codes = {}
        unresolved = []

        for parameter, value in chosen.items():
            if value == "all":
                filter_codes[parameter] = None
                continue

            code = filter_options.get(parameter, {}).get(value)
            if not code:
                unresolved.append(value)
                continue

            filter_codes[parameter] = code
            filters.append({"Type": parameter_facets[parameter], "Values": [code]})

        return filters, filter_codes, unresolved

    @staticmethod
    def map_item(item):
        """
        Map a collected item to a flat row.

        Handles both the Discovery API items collected since 2026-09 and the
        Selenium-scraped items in datasets collected before that, so older
        datasets keep opening and exporting exactly as they did.
        """
        if "listing" in item:
            try:
                return SearchAwsStore.map_api_item(item)
            except (TypeError, AttributeError) as e:
                # 4CAT turns KeyError and IndexError into a MapItemException and
                # alerts an operator; a field that changed type rather than name
                # deserves the same treatment instead of breaking iteration.
                raise KeyError(f"AWS listing has an unexpected structure ({type(e).__name__}: {e})") from e
        return SearchAwsStore.map_scraped_item(item)

    @staticmethod
    def map_api_item(item):
        """
        Map an item collected from the Discovery API.

        Every optional field is read through a helper that tests whether AWS
        *sent the key*, not whether the value is truthy. The three states are
        distinct and are kept distinct:

        - key absent    -> MissingMappedField; AWS told us nothing, so neither
                           can we
        - key present, null or empty -> the default; AWS told us there is no
                           value, which is something we know
        - key present with a value -> the value

        Fields no listing can be useful without go through `required` instead,
        which raises when they are absent or null. That is not a per-listing
        quirk but a sign the response format changed, and raising routes it into
        4CAT's unmappable-item alerting so an operator hears about it.
        """
        listing = item["listing"]
        metadata = item.get("4CAT_metadata", {})

        app_id = SearchAwsStore.required(listing, "Id")
        display = SearchAwsStore.required(listing, "DisplayAttributes")
        title = SearchAwsStore.required(display, "Title", "DisplayAttributes.Title")

        # A container AWS nulled or omitted tells us nothing about its members,
        # so an empty stand-in makes every field inside it read as absent.
        creator = listing.get("ListingCreator") or {}
        offer = listing.get("OfferSummary") or {}
        product = listing.get("ProductAttributes") or {}
        version = display.get("VersionInformation") or {}
        raw_vendor_id = creator.get("ProfileId")

        return MappedItem({
            "id": app_id,
            "query": metadata.get("query", ""),
            "category": metadata.get("category") or "all",
            "creator": metadata.get("creator") or "all",
            "pricing_model": metadata.get("pricing_model") or "all",
            "fulfillment_option_type": metadata.get("fulfillment_option_type") or "all",
            "page": metadata.get("page", ""),
            "rank": metadata.get("rank", ""),
            "timestamp": SearchAwsStore.safe_timestamp(metadata.get("collected_at_timestamp")),
            "body": SearchAwsStore.best_text(display),

            "app_id": app_id,
            "title": title,
            "app_url": SearchAwsStore.listing_url + app_id,
            "vendor_name": SearchAwsStore.optional(creator, "DisplayName"),
            # Derived rather than reported: a profile URL built from an empty id
            # would be wrong, not empty, so there is nothing to report here.
            "vendor_url": (SearchAwsStore.vendor_url + raw_vendor_id) if raw_vendor_id else MissingMappedField(""),
            "badge": SearchAwsStore.first_display_name(listing, "Badges"),
            "pricing": SearchAwsStore.optional(offer, "PricingSummary"),
            "search_description": SearchAwsStore.optional(display, "ShortDescription"),
            "thumbnail": SearchAwsStore.thumbnail_url(listing, "Media"),
            # Generated in the web interface AWS no longer exposes to us; kept as
            # a column so datasets collected before the API switch line up.
            "ai_listing_highlights": MissingMappedField(""),

            "long_description": SearchAwsStore.optional(display, "LongDescription"),
            "categories": SearchAwsStore.display_names(listing, "Categories"),
            "badges": SearchAwsStore.display_names(listing, "Badges"),
            "fulfillment_options": SearchAwsStore.display_names(listing, "FulfillmentOptionTypes"),
            "pricing_models": SearchAwsStore.display_names(offer, "PricingModels"),
            "pricing_units": SearchAwsStore.display_names(offer, "PricingUnits"),
            "purchase_paths": SearchAwsStore.joined_strings(listing, "PurchasePaths"),
            "vendor_id": SearchAwsStore.optional(creator, "ProfileId"),
            "product_id": SearchAwsStore.optional(product, "BaseProductId"),
            "recommended_version": SearchAwsStore.optional(version, "RecommendedVersion"),

            **SearchAwsStore.review_columns(listing, "Reviews"),
        })

    @staticmethod
    def map_scraped_item(item):
        """
        Map an item collected by the pre-2026-09 Selenium scraper.

        Reproduces the original mapping exactly; existing datasets should not
        change shape because the collection method did.
        """
        item = item.copy()
        fourcat_metadata = item.pop("4CAT_metadata", {})
        # Remove HTML source
        item.pop("html_source", None)
        best_text = item.get("search_description") or item.get("ai_listing_highlights") or ""
        return MappedItem({
            "query": fourcat_metadata.get("query", ""),
            "category": fourcat_metadata.get("category") if fourcat_metadata.get("category") else "all",
            "creator": fourcat_metadata.get("creator") if fourcat_metadata.get("creator") else "all",
            "pricing_model": fourcat_metadata.get("pricing_model") if fourcat_metadata.get("pricing_model") else "all",
            "fulfillment_option_type": fourcat_metadata.get("fulfillment_option_type") if fourcat_metadata.get("fulfillment_option_type") else "all",
            "page": fourcat_metadata.get("page", ""),
            "rank": fourcat_metadata.get("rank", ""),
            "timestamp": SearchAwsStore.safe_timestamp(fourcat_metadata.get("collected_at_timestamp")),
            "body": best_text,
            **item
        })

    @staticmethod
    def safe_timestamp(value):
        """
        Coerce a stored collection timestamp to an integer.

        Returns 0 rather than raising: a missing timestamp is a flaw in the
        stored row, not a reason to make the whole item unreadable.
        """
        try:
            return int(value)
        except (TypeError, ValueError):
            return MissingMappedField(0)

    @staticmethod
    def required(container, key, path=None):
        """
        Read a field the data source cannot do without.

        Absent or null here is not a quirk of one listing: `Id` and a title are
        what make a row a row, and AWS has always sent them. Their disappearance
        means the response format changed. Raising KeyError puts that in front of
        an operator through 4CAT's unmappable-item alerting, instead of writing
        thousands of rows with an empty identifier.

        :param dict container:  Object to read from
        :param str key:  Key that must be present and non-null
        :param str path:  Dotted path for the error message, if not just `key`
        :return:  The value
        """
        value = container.get(key) if isinstance(container, dict) else None
        if value is None:
            raise KeyError(
                f"AWS listing has no {path or key}; the API response format has likely changed"
            )
        return value

    @staticmethod
    def optional(container, key, default=""):
        """
        Read a field AWS may not send, keeping absent apart from empty.

        Presence of the key is what is tested, never the truthiness of the
        value, because those answer different questions:

        - the key is absent, so AWS said nothing about this field and neither
          can we -> MissingMappedField, recorded in the `missing_fields` column
        - the key is there and null or empty, so AWS said there is no value
          here, which is knowledge -> the default
        - the key is there with a value -> that value, even when it is falsy

        :param dict container:  Object to read from
        :param str key:  Key to read
        :param default:  Value standing in for an explicit null
        :return:  The value, the default, or MissingMappedField
        """
        if key not in container:
            return MissingMappedField(default)
        value = container[key]
        return default if value is None else value

    @staticmethod
    def display_names(container, key):
        """
        Flatten a list of {"DisplayName": ...} objects to a comma-separated string.

        An empty or null list is AWS saying there are none, which maps to "";
        only an absent key is unknown. A member without a `DisplayName` raises,
        since that is the API changing rather than a listing without categories.
        """
        if key not in container:
            return MissingMappedField("")
        values = container[key]
        return ",".join(value["DisplayName"] for value in values) if values else ""

    @staticmethod
    def first_display_name(container, key):
        """
        The first display name in a list, for the single-valued legacy `badge`
        column. The full list is available in `badges`.
        """
        if key not in container:
            return MissingMappedField("")
        values = container[key]
        return values[0]["DisplayName"] if values else ""

    @staticmethod
    def joined_strings(container, key):
        """
        Flatten a list of plain strings to a comma-separated string.
        """
        if key not in container:
            return MissingMappedField("")
        values = container[key]
        return ",".join(values) if values else ""

    @staticmethod
    def best_text(display):
        """
        The fullest description AWS supplied, for 4CAT's `body` column.

        Unknown only when AWS sent neither description field. A listing that
        carries both keys with nothing in them has an empty body, which is a
        fact about the listing rather than a gap in what we collected.
        """
        if "ShortDescription" not in display and "LongDescription" not in display:
            return MissingMappedField("")
        return display.get("ShortDescription") or display.get("LongDescription") or ""

    @staticmethod
    def thumbnail_url(container, key):
        """
        Build a thumbnail URL from AWS's split domain/path media object.

        A media object carrying no usable thumbnail is a known answer -- this
        listing has no logo -- so it maps to "" rather than being marked unknown.
        """
        if key not in container:
            return MissingMappedField("")
        media = container[key] or {}
        for media_key in ("LOGO_THUMBNAIL", "LISTING_LOGO_THUMBNAIL"):
            image = media.get(media_key)
            if image and image.get("Path"):
                return image.get("DefaultDomain", "") + image["Path"]
        return ""

    # Review sources AWS aggregates, and the column prefix each gets
    review_sources = {"AWS_MARKETPLACE": "aws", "G2": "g2", "PEERSPOT": "peerspot"}

    @staticmethod
    def review_columns(container, key):
        """
        Flatten the per-source review aggregates.

        AWS sends `Reviews: {}` for a listing nobody has reviewed, so a source
        absent from a present `Reviews` object means no reviews there -- a count
        of zero, which we know.

        A null `Reviews` is treated like an absent one, consistent with the other
        nested objects: it means the per-source keys never arrived, so we cannot
        say whether a listing has G2 reviews or simply was not checked.
        """
        columns = {}
        known = container.get(key) is not None
        reviews = container.get(key) or {}

        for source, prefix in SearchAwsStore.review_sources.items():
            if not known:
                columns[f"review_rating_{prefix}"] = MissingMappedField("")
                columns[f"review_count_{prefix}"] = MissingMappedField(0)
                continue
            aggregate = reviews.get(source) or {}
            columns[f"review_rating_{prefix}"] = aggregate.get("AverageCustomerRating", "")
            columns[f"review_count_{prefix}"] = aggregate.get("Count", 0)
        return columns

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
        queries = [q.strip() for q in re.split(r',|\n', query.get('search_query', "")) if q.strip()]
        if not queries:
            # always at least one "query"
            queries = [""]

        chosen = {}
        for parameter in SearchAwsStore.query_param_map.values():
            value = query.get(parameter, ["all"])
            if type(value) is str:
                value = [value]
            chosen[parameter] = value

        total_queries = len(queries)
        for values in chosen.values():
            total_queries *= len(values)
        num_results = int(query.get('amount', 60)) * total_queries

        if not query.get("frontend-confirm") and (total_queries > 50 or num_results > 10000):
            raise QueryNeedsExplicitConfirmationException(f"This combination of filters and queries will result in {total_queries} queries w/ up to {num_results} results. Please confirm you want to proceed.")

        return {
            "search_query": queries,
            **chosen,
            "amount": query.get('amount', 60),
        }


class AwsStoreCategories(BasicWorker):
    """
    Collect AWS Store filter options and store them in the database.

    Previously scraped the search page's left sidebar, which only ever rendered
    the first handful of options per filter and required a browser to see any of
    them. One GetSearchFacets call now returns the whole vocabulary, with a
    listing count per option and the parent/child relation between categories.
    """
    type = "aws-store-category-collector"  # job ID

    # The API will return more (there are thousands of vendors), but these have
    # to fit in a usable dropdown. Options come back ordered by listing count,
    # so this keeps the ones researchers are most likely to want.
    max_options_per_filter = 500

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
        Refresh the filter vocabulary offered by the AWS Marketplace data source.
        """
        if "aws-store" not in self.config.get("datasources.enabled"):
            # Datasource no longer enabled; delete job if it exists and skip
            self.job.finish(delete=True)
            return

        api = AwsMarketplaceApi(logger=self.log)
        facet_types = [facet_type for facet_type, _ in SearchAwsStore.filter_facets.values()]

        try:
            facets = api.get_search_facets(
                facet_types=facet_types,
                max_results_per_facet=self.max_options_per_filter,
            )
        except AwsMarketplaceContractError as e:
            # Keep whatever vocabulary is already cached: a stale filter list is
            # more useful than none, and the interface says how old it is.
            self.log.error(f"AWS Marketplace filter collection failed -- API changed: {e}")
            add_notification(
                self.db, "!admin",
                f"The AWS Marketplace API returned an unexpected response while refreshing filter options, so they "
                f"could not be updated. This usually means AWS changed the API. Details: {e}"
            )
            return
        except AwsMarketplaceApiError as e:
            # Transient; the job runs again tomorrow
            self.log.warning(f"AWS Marketplace filter collection failed: {e}")
            return

        collected = self.facets_to_query_options(facets, logger=self.log)

        if not collected:
            self.log.warning("Failed to collect any filter options from AWS Store")
            return

        self.log.info(
            f"Collected {len(collected)} query types with a total of "
            f"{sum(len(options) for options in collected.values())} options for the AWS Store"
        )

        # Replace whole sections to remove old options, but do not remove
        # sections that were not collected at all, so a partial failure does not
        # silently drop a filter from the interface.
        old_options = self.config.get("cache.aws.query_options", {})
        for filter_name, options in collected.items():
            old_options[filter_name] = options

        self.config.set("cache.aws.query_options", old_options)
        self.config.set("cache.aws.query_options_updated_at", datetime.now().timestamp())

    @staticmethod
    def facets_to_query_options(facets, logger=None):
        """
        Convert an API facet map into the cached query-option format.

        The stored format is unchanged from when these were scraped, so saved
        queries and the option-rendering code keep working:

            {"Categories": [{"name": ..., "data-value": ...}, ...], ...}

        Display names are made unique within each filter. Both the interface and
        the query resolver key options by name, so two options sharing one -- as
        several AWS vendors do, "Amazon Web Services" among them -- would
        otherwise collapse into one and make the rest unreachable.

        :param dict facets:  ListingFacets from GetSearchFacets
        :param logger:  Optional logger
        :return dict:  Filter name -> list of options
        """
        collected = {}

        for filter_name, (facet_type, all_label) in SearchAwsStore.filter_facets.items():
            facet_options = facets.get(facet_type, [])
            if not facet_options:
                if logger:
                    logger.warning(f"AWS Marketplace: facet '{facet_type}' returned no options; keeping previously cached {filter_name}")
                continue

            name_counts = Counter(option["DisplayName"] for option in facet_options)
            options = [{"name": all_label, "data-value": all_label}]
            used_names = {all_label}
            seen_values = set()

            for option in facet_options:
                value = option["Value"]
                if value in seen_values:
                    continue
                seen_values.add(value)

                name = option["DisplayName"]
                if name_counts[name] > 1:
                    # Distinct entries that happen to share a label; qualify with
                    # the start of the identifier that actually distinguishes them
                    name = f"{name} ({value[:8]})"
                # Belt and braces: guarantee the invariant the callers rely on
                unique_name = name
                suffix = 2
                while unique_name in used_names:
                    unique_name = f"{name} ({suffix})"
                    suffix += 1
                used_names.add(unique_name)

                entry = {"name": unique_name, "data-value": value}
                if option.get("Parent"):
                    entry["parent"] = option["Parent"]
                options.append(entry)

            collected[filter_name] = options
            if logger:
                logger.info(f"AWS Marketplace: collected {len(options) - 1} options for {filter_name}")

        return collected
