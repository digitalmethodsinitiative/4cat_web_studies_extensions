"""
Client for the AWS Marketplace Discovery API.

This is the JSON service that aws.amazon.com/marketplace/search/ calls to render
itself. It is not a documented, supported AWS API -- it is the private backend of
the Marketplace web client -- but it needs no credentials, cookies or browser, and
it returns more data than the rendered page does.

    service   com.amazonaws.marketplace.discovery.api.v20200320
    target    AWSMPDiscoveryService
    proxy     POST https://aws.amazon.com/marketplace/api/awsmpdiscovery

The proxy takes an envelope wrapping the real service call:

    {"content": <service payload>, "headers": {"X-Amz-Target": ...}, "region": ...}

Because the endpoint is undocumented, every response is checked against a shallow
contract before it is handed back. The checks are deliberately loose so a new
optional field does not break collection, but a response that no longer carries
the containers we read raises AwsMarketplaceContractError rather than quietly
yielding nothing. Callers are expected to surface that difference: an empty result
set and a changed API are not the same event, and a datasource that cannot tell
them apart would silently rot.
"""

import json
import random
import re
import time

import requests

from common.lib.exceptions import ProcessorException


class AwsMarketplaceApiError(ProcessorException):
    """
    The API could not be reached, or returned an error.

    Transient by assumption: the request may be worth retrying, either
    immediately (the client does this itself) or on the next scheduled run.
    """


class AwsMarketplaceContractError(AwsMarketplaceApiError):
    """
    The API responded, but not in the shape this client was written against.

    Never retried -- retrying a schema change just delays the diagnosis. Raised
    with enough context (operation, the path that failed, a truncated sample) to
    tell someone what AWS changed.
    """


class AwsMarketplaceApi:
    """
    Thin client for the operations this datasource needs.

    Not a general-purpose SDK: SearchListings and GetSearchFacets are what the
    AWS Marketplace datasource collects. The service also exposes
    GetListingView, GetSearchTextSuggestions, ListListingViewQueries and
    ListPrivateListings (the last requires an authenticated account).
    """
    base_url = "https://aws.amazon.com/marketplace/"
    search_page_url = base_url + "search/"
    api_url = base_url + "api/awsmpdiscovery"

    service_target = "AWSMPDiscoveryService"
    api_version = "2020-03-20"
    region = "us-east-1"
    locale = "en_US"

    # Required by the service, and baked into the Marketplace web client. AWS
    # also prints it verbatim in the served HTML of the search page, so it can
    # be re-derived at runtime; this constant is the fallback for when that
    # lookup fails.
    fallback_integration_id = "integ-kzv5xkq73lzjw"
    integration_id_pattern = re.compile(r"integ-[a-z0-9]{8,}")

    # Server-enforced: MaxResults above this is rejected outright with a
    # ValidationException.
    max_page_size = 100

    # Retries apply to throttling, server errors and connection failures only.
    max_attempts = 4
    backoff_base = 2
    max_backoff = 60

    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0"

    def __init__(self, logger=None, timeout=60):
        """
        :param logger:  Optional logger, used for non-fatal observations
        :param int timeout:  Per-request timeout in seconds
        """
        self.log = logger
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.user_agent})
        self._integration_id = None

    def integration_id(self):
        """
        Resolve the IntegrationId the service requires.

        Read from the live search page so a rotation on AWS's side fixes itself,
        with the known constant as a fallback. A difference between the two is
        logged rather than passed over: it is the only warning we would get
        before the constant goes stale.

        :return str:  IntegrationId
        """
        if self._integration_id:
            return self._integration_id

        discovered = None
        try:
            response = self.session.get(self.search_page_url, timeout=self.timeout)
            response.raise_for_status()
            match = self.integration_id_pattern.search(response.text)
            if match:
                discovered = match.group(0)
            elif self.log:
                self.log.warning("AWS Marketplace: no IntegrationId found in search page HTML; using fallback")
        except requests.RequestException as e:
            if self.log:
                self.log.warning(f"AWS Marketplace: could not read IntegrationId from search page ({type(e).__name__}: {e}); using fallback")

        if discovered and discovered != self.fallback_integration_id and self.log:
            self.log.info(f"AWS Marketplace: IntegrationId has changed from {self.fallback_integration_id} to {discovered}; update the fallback constant")

        self._integration_id = discovered or self.fallback_integration_id
        return self._integration_id

    def call(self, operation, payload):
        """
        Invoke one Discovery API operation through the public proxy.

        Retries throttling, server errors and connection failures with
        exponential backoff, honouring Retry-After when the service supplies it.
        A 400 is not retried: the service only returns one for a malformed
        request, which is our bug, and retrying would bury it.

        :param str operation:  Operation name, e.g. "SearchListings"
        :param dict payload:  Service payload; RequestContext is added here
        :return dict:  Parsed response body
        """
        envelope = {
            "content": {
                "RequestContext": {"IntegrationId": self.integration_id(), "Locale": self.locale},
                **payload,
            },
            "headers": {
                "X-Amz-Target": f"{self.service_target}.{operation}",
                "Content-Type": "application/x-amz-json-1.1",
            },
            "region": self.region,
        }

        last_error = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                response = self.session.post(
                    self.api_url,
                    json=envelope,
                    headers={"Content-Type": "application/json"},
                    timeout=self.timeout,
                )
            except requests.RequestException as e:
                last_error = f"{type(e).__name__}: {e}"
                self._sleep_before_retry(attempt, operation, last_error)
                continue

            if response.status_code == 200:
                try:
                    body = response.json()
                except (ValueError, json.JSONDecodeError) as e:
                    # A 200 that is not JSON means something is answering that
                    # is not the service -- a captive portal or an error page.
                    raise AwsMarketplaceContractError(
                        f"{operation} returned a non-JSON 200 response ({type(e).__name__}); "
                        f"first 200 bytes: {response.text[:200]!r}"
                    )
                if not isinstance(body, dict):
                    raise AwsMarketplaceContractError(
                        f"{operation} returned {type(body).__name__} at the top level, expected an object"
                    )
                return body

            if response.status_code == 400:
                raise AwsMarketplaceApiError(
                    f"{operation} rejected as invalid (HTTP 400): {response.text[:400]}"
                )

            # 429 and 5xx are worth another go; anything else (403, 404) is not
            # going to change by itself, but is still reported the same way once
            # attempts run out.
            last_error = f"HTTP {response.status_code}: {response.text[:200]}"
            if response.status_code not in (429,) and response.status_code < 500:
                raise AwsMarketplaceApiError(f"{operation} failed -- {last_error}")

            self._sleep_before_retry(attempt, operation, last_error, response=response)

        raise AwsMarketplaceApiError(
            f"{operation} failed after {self.max_attempts} attempts -- {last_error}"
        )

    def _sleep_before_retry(self, attempt, operation, error, response=None):
        """
        Back off before the next attempt, honouring Retry-After if given.

        Does not sleep after the final attempt -- the caller is about to raise.

        :param int attempt:  1-based attempt number that just failed
        :param str operation:  Operation name, for logging
        :param str error:  What went wrong, for logging
        :param response:  Response object, if one was received
        """
        if attempt >= self.max_attempts:
            return

        delay = None
        if response is not None:
            retry_after = response.headers.get("Retry-After")
            if retry_after:
                try:
                    delay = float(retry_after)
                except ValueError:
                    delay = None
            if delay is None:
                try:
                    delay = float(response.json().get("RetryAfterSeconds"))
                except (ValueError, AttributeError, TypeError, json.JSONDecodeError):
                    delay = None

        if delay is None:
            # Jittered exponential backoff, so parallel workers do not retry in
            # lockstep.
            delay = (self.backoff_base ** attempt) + random.uniform(0, 1)

        delay = min(delay, self.max_backoff)
        if self.log:
            self.log.warning(f"AWS Marketplace: {operation} attempt {attempt}/{self.max_attempts} failed ({error}); retrying in {delay:.1f}s")
        time.sleep(delay)

    def search_listings(self, search_text=None, filters=None, max_results=None,
                        page_size=None, interrupt_check=None, delay=0.2):
        """
        Yield listings for a query, following NextToken until AWS stops.

        Yields (listing, total_results, page_number) so callers can record where
        in the result set an item came from without counting themselves.
        `total_results` is what AWS reports for the whole query, not what has
        been collected so far.

        Ids already seen are tracked and skipped: the index can shift between
        pages of a long crawl, and a duplicate is worth a warning rather than a
        silently doubled row.

        :param str search_text:  Free-text query, or None for everything
        :param list filters:  [{"Type": ..., "Values": [...]}, ...]
        :param int max_results:  Stop after this many; None to exhaust the query
        :param int page_size:  Results per request, capped at max_page_size
        :param callable interrupt_check:  Called between pages; should raise to abort
        :param float delay:  Seconds to pause between pages
        :return generator:
        """
        page_size = min(page_size or self.max_page_size, self.max_page_size)
        if max_results:
            page_size = min(page_size, max_results)

        collected = 0
        page = 0
        next_token = None
        seen_ids = set()
        duplicates = 0

        while True:
            if interrupt_check:
                interrupt_check()

            payload = {"MaxResults": page_size}
            if search_text:
                payload["SearchText"] = search_text
            if filters:
                payload["Filters"] = filters
            if next_token:
                payload["NextToken"] = next_token

            response = self.call("SearchListings", payload)
            listings, total_results, next_token = self._validate_search_listings(response)
            page += 1

            for listing in listings:
                listing_id = listing.get("Id")
                if listing_id and listing_id in seen_ids:
                    duplicates += 1
                    continue
                if listing_id:
                    seen_ids.add(listing_id)

                yield listing, total_results, page
                collected += 1
                if max_results and collected >= max_results:
                    self._warn_duplicates(duplicates, search_text)
                    return

            if not next_token or not listings:
                break

            if delay:
                time.sleep(delay)

        self._warn_duplicates(duplicates, search_text)

    def _warn_duplicates(self, duplicates, search_text):
        """
        Report listings AWS handed back more than once during one crawl.

        Not an error -- the result set genuinely shifts under deep pagination as
        listings are added and removed -- but it means the collection is not a
        clean snapshot, which matters for anyone counting.
        """
        if duplicates and self.log:
            self.log.warning(
                f"AWS Marketplace: {duplicates} duplicate listing(s) skipped while paginating "
                f"{'query ' + repr(search_text) if search_text else 'unfiltered query'}; "
                f"the result set shifted during collection"
            )

    def get_search_facets(self, facet_types=None, max_results_per_facet=500,
                          search_text=None, filters=None):
        """
        Fetch the filter vocabulary (categories, vendors, pricing, delivery).

        This is what the search page's left sidebar renders, except the sidebar
        only ever shows the first handful per facet.

        :param list facet_types:  Facet names to fetch, or None for all of them
        :param int max_results_per_facet:  Options per facet
        :param str search_text:  Scope facets to a query, or None for the whole store
        :param list filters:  Scope facets to existing filters
        :return dict:  Facet name -> list of {"Value", "DisplayName", "Count", ...}
        """
        payload = {"MaxResultsPerFacet": max_results_per_facet}
        if facet_types:
            payload["FacetTypes"] = facet_types
        if search_text:
            payload["SearchText"] = search_text
        if filters:
            payload["Filters"] = filters

        response = self.call("GetSearchFacets", payload)
        return self._validate_search_facets(response, facet_types)

    @staticmethod
    def _sample(response):
        """
        A short, safe preview of a response, for contract error messages.
        """
        try:
            return json.dumps(response)[:300]
        except (TypeError, ValueError):
            return repr(response)[:300]

    @classmethod
    def _validate_search_listings(cls, response):
        """
        Check a SearchListings response and unpack the parts we use.

        Only the containers are checked. Individual listings are validated where
        they are read, in the datasource's map_item, so that one malformed
        listing costs one row rather than the whole dataset.

        :param dict response:  Parsed response body
        :return tuple:  (listings, total_results, next_token)
        """
        listings = response.get("ListingSummaries")
        if listings is None:
            raise AwsMarketplaceContractError(
                f"SearchListings response has no 'ListingSummaries'; keys were "
                f"{sorted(response.keys())}. Sample: {cls._sample(response)}"
            )
        if not isinstance(listings, list):
            raise AwsMarketplaceContractError(
                f"SearchListings 'ListingSummaries' is {type(listings).__name__}, expected a list"
            )

        total_results = response.get("TotalResults")
        if total_results is not None and not isinstance(total_results, int):
            raise AwsMarketplaceContractError(
                f"SearchListings 'TotalResults' is {type(total_results).__name__}, expected an integer"
            )

        next_token = response.get("NextToken")
        if next_token is not None and not isinstance(next_token, str):
            raise AwsMarketplaceContractError(
                f"SearchListings 'NextToken' is {type(next_token).__name__}, expected a string"
            )

        return listings, total_results, next_token

    @classmethod
    def _validate_search_facets(cls, response, requested_facets=None):
        """
        Check a GetSearchFacets response and return its facet map.

        A requested facet that comes back missing is a contract failure: it
        means AWS renamed or retired a filter the datasource offers, and
        continuing would quietly drop that filter from the interface.

        :param dict response:  Parsed response body
        :param list requested_facets:  Facets that were asked for, if any
        :return dict:  ListingFacets
        """
        facets = response.get("ListingFacets")
        if facets is None:
            raise AwsMarketplaceContractError(
                f"GetSearchFacets response has no 'ListingFacets'; keys were "
                f"{sorted(response.keys())}. Sample: {cls._sample(response)}"
            )
        if not isinstance(facets, dict):
            raise AwsMarketplaceContractError(
                f"GetSearchFacets 'ListingFacets' is {type(facets).__name__}, expected an object"
            )

        for facet_name, options in facets.items():
            if not isinstance(options, list):
                raise AwsMarketplaceContractError(
                    f"GetSearchFacets facet '{facet_name}' is {type(options).__name__}, expected a list"
                )
            for option in options:
                if not isinstance(option, dict) or "Value" not in option or "DisplayName" not in option:
                    raise AwsMarketplaceContractError(
                        f"GetSearchFacets facet '{facet_name}' has an option without "
                        f"'Value'/'DisplayName': {cls._sample(option)}"
                    )

        missing = [facet for facet in (requested_facets or []) if facet not in facets]
        if missing:
            raise AwsMarketplaceContractError(
                f"GetSearchFacets did not return requested facet(s) {missing}; "
                f"available facets were {sorted(facets.keys())}"
            )

        return facets
