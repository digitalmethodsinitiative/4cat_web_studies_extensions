"""
Web Archives Screenshot collector

Currently designed around Firefox, but can also work with Chrome; results may vary
"""
import datetime
import json

from extensions.web_studies.datasources.web_archive_scraper.search_web_archive import SearchWebArchiveWithSelenium
from common.lib.user_input import UserInput
from common.lib.helpers import url_to_hash


class _MissingAttribute:
    """
    Descriptor to make a specific attribute appear missing (hasattr -> False).
    (Some 4CAT methods check for `map_item` and this ought not have it)

    """
    def __init__(self, name: str = ""):
        self.name = name

    def __get__(self, obj, owner):
        # Cause getattr/hasattr to treat this attribute as missing
        raise AttributeError(self.name or "attribute missing")


class ScreenshotWebArchiveWithSelenium(SearchWebArchiveWithSelenium):
    """
    Screenshot pages from Web Archive (web.archive.org) via the Selenium webdriver and Firefox browser
    """
    type = "web_archive_screenshot-search"  # job ID
    title = "Web Archive Screenshot"
    category = "Search"  # category
    description = "Screenshot pages from the Web Archive (web.archive.org) using Selenium and Firefox"
    extension = "zip"
    media_type = "image"

    @classmethod
    def get_options(cls, parent_dataset=None, config=None):
        """
        Interface options for ScreenshotWebArchiveWithSelenium
        """
        options = {
            "intro-1": {
                "type": UserInput.OPTION_INFO,
                "help": "Screenshot pages from the Web Archive (web.archive.org) using Selenium and Firefox. By setting a frequency, you can collect multiple snapshots of the same page over time."
            },
            "query-info": {
                "type": UserInput.OPTION_INFO,
                "help": "Please enter a list of urls one per line."
            },
            "query": {
                "type": UserInput.OPTION_TEXT_LARGE,
                "help": "List of urls"
            },
            "frequency": {
                "type": UserInput.OPTION_CHOICE,
                "help": "Frequency over time period",
                "tooltip": "Default 'First Available' scrapes the first available result after start date",
                "options": {
                    "first": "First Available",
                    "monthly": "Monthly",
                    "weekly": "Weekly",
                    "daily": "Daily",
                    "yearly": "Yearly"
                },
                "default": "first"
            },
            "daterange": {
                "type": UserInput.OPTION_DATERANGE,
                "tooltip": "Scrapes first available page after start date; Uses start and end date for frequency",
                "help": "Date range"
            },
            "pause-time": {
                "type": UserInput.OPTION_TEXT,
                "help": "Pause time",
                "tooltip": "Before each screenshot, wait this many seconds before taking the screenshot. This can help "
                           "with images loading.",
                "default": 6,
                "min": 2,
                "max": 30,
            },
        }

        return options

    def get_items(self, query):
        """
        Iterate successful snapshot-per-segment events and capture screenshots.
        Returns the staging directory path after writing a .metadata.json manifest.
        """
        # Staging area where screenshots will be collected
        staging = self.dataset.get_staging_area()
        wait = query.get("pause-time", 6)

        metadata = {}
        screenshots = 0
        for event in self.iter_snapshots(query):
            if not event.get('ok'):
                # record failure in metadata for transparency
                seg_start = event['seg_start']
                metadata_key = event['url'] + '@' + (seg_start.strftime('%Y%m%d%H%M%S') if seg_start else 'segment')
                metadata[metadata_key] = {
                    'url': event['url'],
                    'fail_url': event['fail_url'],
                    'error': event.get('error', ''),
                }
                continue

            # Successful snapshot: take screenshot
            dt = event['dt']
            snapshot_url = event['snapshot_url']
            base_url = event['url']
            ts_slug = dt.strftime('%Y%m%d%H%M%S') if dt else 'unknown'
            filename = f"screenshot_{ts_slug}_{url_to_hash(snapshot_url)}.png"
            out_path = staging.joinpath(filename)

            error = ''
            try:
                # Full page if possible
                # `wait` is hard sleep, page ought to be loaded, but may need to increase wait for images
                self.save_screenshot(out_path, wait=wait, viewport_only=False)
                screenshots += 1
            except Exception as e:
                error = str(e)

            metadata_key = base_url + '@' + ts_slug
            metadata[metadata_key] = {
                'url': base_url,
                'snapshot_url': snapshot_url,
                'filename': filename,
                'final_url': self.driver.current_url,
                'subject': self.driver.title,
                'timestamp': int(datetime.datetime.now().timestamp()),
                'error': error,
            }

        # Write manifest
        try:
            with staging.joinpath('.metadata.json').open('w', encoding='utf-8') as f:
                json.dump(metadata, f)
        except Exception:
            self.dataset.log("Failed to write metadata manifest")
            self.log.error("Failed to write metadata manifest for Web Archive")


        # Update status and return the path to the staging area (consistent with search_webpage_screenshots behavior)
        self.dataset.update_status("Compressing images")
        return staging
  
    # Intentionally hide the inherited 'map_item' so hasattr(..., 'map_item') returns False for this zip of images
    map_item = _MissingAttribute('map_item')

