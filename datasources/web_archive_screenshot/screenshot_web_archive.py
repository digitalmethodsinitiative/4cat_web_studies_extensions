"""
Web Archives Screenshot collector

Currently designed around Firefox, but can also work with Chrome; results may vary
"""
import datetime
import json

from extensions.web_studies.datasources.url_screenshots.search_webpage_screenshots  import ScreenshotWithSelenium
from extensions.web_studies.datasources.web_archive_scraper.search_web_archive import SearchWebArchiveWithSelenium
from common.lib.user_input import UserInput


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
                "help": "Make screenshots of archived versions of a website using the Internet Archive's "
                        "[Wayback Machine](https://web.archive.org/). These are opened with a Firefox browser. By setting a screenshot interval, "
                        "you can collect multiple snapshots of the same page over time.\n\nNote that the "
                        "Wayback Machine is a (very) *slow* website and screenshot wait times add up quickly; "
                        "it is recommended to start 'zoomed out' (e.g. with yearly screenshots) and use a "
                        "narrow date range for higher frequencies."
            },
            "query-info": {
                "type": UserInput.OPTION_INFO,
                "help": "Please enter a list of URLs, one per line."
            },
            "query": {
                "type": UserInput.OPTION_TEXT_LARGE,
                "help": "List of URLs",
                "tooltip": "These should be the URLs to collect archived versions of. Separate with commas or new lines."
            },
            "frequency-info": {
                "type": UserInput.OPTION_INFO,
                "help": "At what interval should screenshots be captured? 'First Available' collects the single "
                        "earliest available version per URL."
            },
            "frequency": {
                "type": UserInput.OPTION_CHOICE,
                "help": "Interval",
                "options": {
                    "first": "First Available",
                    "yearly": "Yearly",
                    "monthly": "Monthly",
                    "weekly": "Weekly",
#                    "daily": "Daily",
                },
                "default": "first"
            },
            "daterange": {
                "type": UserInput.OPTION_DATERANGE,
                "tooltip": "Scrapes first available page after start date; if capturing at an interval, "
                           "archived versions between these dates are included.",
                "help": "Date range"
            },
            "pause-time": {
                "type": UserInput.OPTION_TEXT,
                "help": "Pause time",
                "tooltip": "Before each screenshot, wait this many seconds before taking the screenshot. This can help "
                           "with pages that are slow to load completely.",
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
        failures = 0
        for event in self.iter_snapshots(query):
            if not event.get('ok'):
                # record failure in metadata for transparency
                failures += 1
                seg_start = event['seg_start']
                metadata_key = event['url'] + '@' + (seg_start.strftime('%Y%m%d%H%M%S') if seg_start else 'segment')
                datetimestr = seg_start.strftime('%Y-%m-%d %H:%M:%S') if seg_start else None
                self.dataset.update_status(f"Failed snapshot: {event['url']}{' @ ' + datetimestr if datetimestr else ''}")
                self.dataset.log(f"Failed snapshot: {event['url']}{' @ ' + datetimestr if datetimestr else ''} - {event.get('error', '')}")
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
            filename = ScreenshotWithSelenium.filename_from_url(snapshot_url) + ".png"
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
        if failures:
            self.dataset.update_status(f"Completed with {screenshots} screenshots and {failures} failures; see log for details", is_final=True)
        return staging
  
    # Intentionally hide the inherited 'map_item' so hasattr(..., 'map_item') returns False for this zip of images
    map_item = _MissingAttribute('map_item')

