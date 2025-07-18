"""
Search HTML for scripts in matching tracking tools
"""
import shutil
import csv
import json
import re
import requests
import subprocess
import sys
from datetime import datetime
from multiprocessing import Pool

from backend.lib.processor import BasicProcessor
from backend.lib.worker import BasicWorker
from common.lib.exceptions import WorkerInterruptedException
from common.lib.helpers import UserInput


__author__ = "Dale Wahl"
__credits__ = ["Dale Wahl"]
__maintainer__ = "Dale Wahl"
__email__ = "4cat@oilab.eu"

csv.field_size_limit(1024 * 1024 * 1024)

# Regex to match URLs; does not include query strings or fragments
url_regex = re.compile(r"https?:\/\/[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(?=[\/\:\#]|$)", re.UNICODE)
def match_trackers(args):
    """
    Check if the substring is in the value and then check if any regex patterns match. First check for
    the substring to speed up the search. If the substring is not found, skip regex matching.

    :param substring: Substring to check for
    :param regex_list: List of regex patterns to check against
    :param value: Value to check against
    :return: List of tuples containing the regex pattern and the pattern key
    """
    substring, regex_list, potential_urls = args
    # Extract URLs from the value
    matches = set()
    # Check if the substring is in the potential URLs
    for potential_url in potential_urls:
        if substring in potential_url:
            for regex in regex_list:
                if regex["regex_pattern"].search(potential_url): # Use the full match from the broad regex
                    matches.add((regex["regex_pattern"].pattern, regex["pattern_key"]))
    return matches


class DetectTrackers(BasicProcessor):
    """
    Detect tracker scripts (from a collection) within collected HTML
    """
    type = "tracker-extractor"  # job type ID
    category = "Post metrics"  # category
    title = "Detect Trackers"  # title displayed in UI
    description = "Identifies URL patterns in HTML or text identified by Ghostery to be used by tracking tools in the selected column. A row for each detected tracker is created in the results."  # description displayed in UI
    extension = "csv"  # extension of result file, used internally and in UI

    references = [
        "[Ghostery](https://www.ghostery.com/)",	
        "[Ghostery tracker database](https://github.com/ghostery/trackerdb?tab=readme-ov-file#ghostery-tracker-database)"
    ]

    possible_parent_columns_for_results = ["id", "timestamp", "url", "final_url", "subject"]

    options = {
        "column": {
            "type": UserInput.OPTION_CHOICE,
            "default": "html",
            "help": "Dataset column containing HTML"
        }
    }

    @classmethod
    def is_compatible_with(cls, module=None, config=None):
        """
        Allow processor on datasets.

        Note: It only makes sense to run on HTML. We may wish to make this more
        specific.

        :param module: Dataset or processor to determine compatibility with
        """
        trackerdb_file = config.get("PATH_ROOT").joinpath("config/ghostery/dist/trackerdb.json")
        return trackerdb_file.exists() and module.get_extension() in ["csv", "ndjson"]

    @classmethod
    def get_options(cls, parent_dataset=None, config=None):
        options = cls.options
        if not parent_dataset:
            return options
        parent_columns = parent_dataset.get_columns()

        if parent_columns:
            parent_columns = {c: c for c in sorted(parent_columns)}
            options["column"] = {
                "type": UserInput.OPTION_CHOICE,
                "options": parent_columns,
                "default": "html" if "html" in parent_columns else "body",
                "help": "Dataset column containing HTML"
        }

        return options

    def process(self):
        """
        Reads a dataset, filtering items that match in the required way, and
        creates a new dataset containing the matching values
        """
        column = self.parameters.get("column", "")

        self.dataset.update_status('Loading trackers...')
        trackerdb_file = self.config.get("PATH_ROOT").joinpath("config/ghostery/dist/trackerdb.json")
        if not trackerdb_file.exists():
            self.dataset.finish_with_error("Ghostery tracker database not found. Please run the Ghostery data updater first.")
            return
        trackersdb = self.load_trackers(trackerdb_file)
        num_trackers = sum([len(regex_list) for substring, regex_list in trackersdb["regex_patterns"].items()])
        self.dataset.update_status('Loaded %i regex tracker patterns.' % num_trackers)
        self.dataset.log('Searching for trackers in column %s' % column)
        matching_items = 0
        missed_items = []
        trackers_found = 0
        
        with self.dataset.get_results_path().open("w", encoding="utf-8") as outfile:
            writer = None

            with Pool() as pool:
                for i, item in enumerate(self.source_dataset.iterate_items(self)):
                    if self.interrupted:
                        raise WorkerInterruptedException("Interrupted while searching for trackers")
                    
                    if column not in item:
                        self.dataset.finish_with_error("Column '%s' not found in dataset" % column)
                        return
                    
                    value = item.get(column)
                    if not value:
                        # No value in column, skip
                        missed_items.append(self.get_item_label(item))
                        continue
                    elif not isinstance(value, str):
                        value = str(value)

                    self.dataset.update_progress(i/self.source_dataset.num_rows)
                    self.dataset.update_status("Searching for trackers in item %i of %i" % (i+1, self.source_dataset.num_rows))
                    
                    # Extract URLs from the value
                    potential_urls = set(url_regex.findall(value))

                    # Search for trackers
                    results = pool.map(
                        match_trackers,
                        [(substring, regex_list, potential_urls) for substring, regex_list in trackersdb["regex_patterns"].items()]
                    )
                    matches = [match for sublist in results for match in sublist]
                            
                    if matches:
                        matching_items += 1
                        result = {
                            "column_searched": column,
                        }
                        # Add item information
                        for key in self.possible_parent_columns_for_results:
                            if key in item:
                                result[key] = item[key]
                        
                        for match in matches:
                            trackers_found += 1
                            pattern_found = trackersdb["patterns"].get(match[1], {})
                            result["tracker_name"] = pattern_found.get("name", "")
                            result["tracker_website"] = pattern_found.get("website_url", "")
                            result["tracker_alias"] = pattern_found.get("alias", "")
                            result["tracker_pattern_matched"] = match[0]
                            
                            category = trackersdb["categories"].get(pattern_found.get("category")) if pattern_found.get("category") else {}
                            result["category"] = category.get("name", "")
                            result["category_description"] = category.get("description", "")

                            organization = trackersdb["organizations"].get(pattern_found.get("organization")) if pattern_found.get("organization") else {}
                            result["organization"] = organization.get("name", "")
                            result["org_description"] = organization.get("description", "")
                            result["org_country"] = organization.get("country", "")
                            result["org_privacy_policy_url"] = organization.get("privacy_policy_url", "")
                            result["org_privacy_contact"] = organization.get("privacy_contact", "")

                            if not writer:
                                writer = csv.DictWriter(outfile, fieldnames=result.keys())
                                writer.writeheader()
                            writer.writerow(result)                

        if matching_items == 0:
            self.dataset.update_status("No items matched your criteria", is_final=True)
        if missed_items:
            for item in missed_items:
                self.dataset.log("No matches in column '%s' for item: %s" % (column, item))
            self.dataset.update_status("Not all items had matches in column '%s'; see log for details" % column, is_final=True)

        self.dataset.finish(trackers_found)

    @staticmethod
    def load_trackers(trackerdb_file):
        """
        This takes a json database of and extracts two possible filters for trackers: host paths and regex patterns.
        The regex_patherns are more presice, but are incredibly time consuming to search for.

        The regex patterns are formmated with a substring and a dictionary with the pattern key and the regex pattern.
        e.g. {"substring": {"pattern_key": "pattern_key", "regex_pattern": "regex_pattern"}}
        This can be used to speed up search; only using the regex if the substring is found.

        The document is found from Ghostery's https://github.com/ghostery/trackerdb repository. Building the database 
        creates a file called "trackerdb.json" which is used as the input to this file. It also contains data on the
        organizations and the categories of the trackers.
        """


        def adblock_to_regex(filter_rule):
            """
            Converts an Adblock Plus filter rule to a regex pattern.
            """
            # Clean the filter rule
            cleaned_rule = filter_rule.replace("||", "")
            cleaned_rule = re.sub(r"\^.*", "", cleaned_rule)
            cleaned_rule = re.sub(r"\$.*", "", cleaned_rule)

            # Escape the cleaned filter rule
            pattern = re.escape(filter_rule)
            # Handle the `||` prefix (matches any subdomain)
            pattern = pattern.replace(r"\|\|", r"")
            # Handle `^` as a boundary marker
            pattern = pattern.replace(r"\^", r"")
            # Remove special rules like `\$3p`
            pattern = re.sub(r"\\\$\w+", "", pattern)

            compiled_pattern = re.compile(pattern)

            return (cleaned_rule, compiled_pattern)

        with trackerdb_file.open() as update:
            trackerdb = json.load(update)

        if any([key not in trackerdb for key in ["domains", "filters"]]):
            raise ValueError("trackerdb.json is missing required keys")
            
        regex_patterns = {}
        for domain, pattern_key in trackerdb["domains"].items():
            # Create regex pattern for domains
            regex_pattern = re.escape(domain)
            compiled_pattern = re.compile(regex_pattern)
            if domain in regex_patterns:
                regex_patterns[domain].append({"pattern_key": pattern_key, "regex_pattern": compiled_pattern})
            else:
                regex_patterns[domain] = [{"pattern_key": pattern_key, "regex_pattern": compiled_pattern}]

        for filter_pattern, pattern_key in trackerdb["filters"].items():
            # Create regex pattern for filters
            substring, regex_pattern = adblock_to_regex(filter_pattern)
            if substring in regex_patterns:
                regex_patterns[substring].append({"pattern_key": pattern_key, "regex_pattern": regex_pattern})
            else:
                regex_patterns[substring] = [{"pattern_key": pattern_key, "regex_pattern": regex_pattern}]

        # Add back organization and category information
        trackerdb["regex_patterns"] = regex_patterns
        
        return trackerdb
    
    @classmethod
    def get_item_label(cls, item):
        """
        Return useful label for item
        """
        label = []
        for key in cls.possible_parent_columns_for_results:
            if key in item:
                label.append(str(item[key]))
        return " - ".join(label)

class GhosteryDataUpdater(BasicWorker):
    """
    Collect Google Cloud Product Store categories and store them in database
    """
    type = "ghostery-data-collector"  # job ID

    repo_url = "https://github.com/ghostery/trackerdb.git"
    repo_latest_release = "https://api.github.com/repos/ghostery/trackerdb/releases/latest"

    config = {
        "cache.ghostery.db_updated_at": {
            "type": UserInput.OPTION_TEXT,
            "help": "Ghostery trackerdb updated at",
            "tooltip": "automatically updated",
            "default": 0,
            "coerce_type": float,
            "indirect": True
        },
        "cache.ghostery.current_release": {
            "type": UserInput.OPTION_TEXT,
            "help": "Ghostery trackerdb release",
            "tooltip": "automatically updated",
            "default": "",
            "indirect": True
        }
    }

    @classmethod
    def ensure_job(cls, config=None):
        """
        Ensure job is scheduled to run every day
        """
        # Run every day to update categories
        if sys.platform == "linux":
            # Only queue job if system is linux
            return {"remote_id": "ghostery-data-collector", "interval": 86400}
        return None

    def ensure_node_installed(self):
        if shutil.which("node") and shutil.which("npm"):
            return True # Node.js and npm are installed
        
        # Check we are in linux environment
        if sys.platform != "linux":
            raise ValueError("This installation is only for Linux OS")
        
        # Install Node.js and npm
        result = subprocess.run(["apt", "update"], capture_output=True)
        if result.returncode != 0:
            raise ValueError("Error updating apt")
        result = subprocess.run(["apt", "install", "-y", "nodejs", "npm"], capture_output=True)
        if result.returncode != 0:
            raise ValueError("Error installing Node.js and npm")
        
        return True
    
    def build_tracker_db(self, ghostery_repo, trackerdb_file):
        # Clone Ghostery tracker database
        try:
            self.ensure_node_installed()
        except ValueError as e:
            self.log.error(f"Error: {e}\nPlease download Ghostery tracker database manually.\nInstructions available at https://github.com/digitalmethodsinitiative/4cat_web_studies_extensions/blob/main/processors/README.md")
            return False
        
        # Build Ghostery tracker database dependencies
        result = subprocess.run(["npm", "install"], capture_output=True, cwd=ghostery_repo)
        if result.returncode != 0:
            self.log.error("Error installing Ghostery tracker database")
            return False
        
        # Create trackerdb.json file
        result = subprocess.run(["node", "scripts/export-json/index.js"], capture_output=True, cwd=ghostery_repo)
        if result.returncode != 0:
            self.log.error("Error building Ghostery tracker database")
            return False

        # Check trackerdb.json file exists
        if not trackerdb_file.exists():
            self.log.error("trackerdb.json file not found")
            return False
        
        return True

    def get_latest_release(self):
        """Fetch the latest release version from GitHub API."""
        response = requests.get(self.repo_latest_release)
        
        if response.status_code == 200:
            data = response.json()
            return data.get("tag_name")
        else:
            self.log.error(f"Ghoserty DB Update Error fetching release: {response.status_code} - {response.text}")
            return None
        
    def work(self):
        ghostery_repo = self.config.get("PATH_ROOT").joinpath("config/ghostery")
        trackerdb_file = ghostery_repo.joinpath("dist/trackerdb.json")
        
        if not ghostery_repo.exists():
            self.log.info("Cloning Ghostery tracker database and installing")
            # First time running, clone the repository
            latest_release = self.get_latest_release()
            result = subprocess.run(["git", "clone", self.repo_url, ghostery_repo])
            if result.returncode != 0:
                self.log.error("Error cloning Ghostery tracker database")
                return
            self.config.set("cache.ghostery.current_release", latest_release)
            self.config.set("cache.ghostery.db_updated_at", datetime.now().timestamp())
            
            # Build the database
            success = self.build_tracker_db(ghostery_repo, trackerdb_file)
            if success:
                self.log.info("Ghostery tracker database installed")

        else:
            # Check if update is needed
            latest_release = self.get_latest_release()
            if not latest_release:
                return
            
            current_release = self.config.get("cache.ghostery.current_release")
            
            if current_release != latest_release:
                self.log.info(f"Updating Ghostery tracker database from {current_release} to {latest_release}")
                # Update the repository
                result = subprocess.run(["git", "pull"], cwd=ghostery_repo)
                if result.returncode != 0:
                    self.log.error("Error updating Ghostery tracker database")
                    return
                self.config.set("cache.ghostery.current_release", latest_release)
                self.config.set("cache.ghostery.db_updated_at", datetime.now().timestamp())

                # Build the database
                self.build_tracker_db(ghostery_repo=ghostery_repo, trackerdb_file=trackerdb_file)
            else:
                self.log.info("Ghostery tracker database is already up to date")
