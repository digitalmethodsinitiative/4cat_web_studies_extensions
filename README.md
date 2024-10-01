# Web Studies a 4CAT Extension
Web Studies is a companion extension to the [4CAT Capture and Analysis Toolkit](https://github.com/digitalmethodsinitiative/4cat?tab=readme-ov-file#-4cat-capture-and-analysis-toolkit). It add functionality to 4CAT by utilizing [Selenium](https://www.selenium.dev/) along with a [Firefox browser](https://www.mozilla.org/en-US/firefox/) to collect data from web sources.

# Features
## New datasources
### General web studies
- Selenium URL Collector
  - Collect HTML, text, and links from a list of URLs
- Web Archive Collector
  - Use [Web Archive's Wayback Machine](https://web.archive.org/) to collect archives of a URL over time
- Screenshot Generator
  - Take screenshots of web pages
### App store studies
- Apple Store
  - Collect data on [Apple's](https://www.apple.com) apps
- Google Store
  - Collect data on [Google's](https://play.google.com/store/apps) apps
### Cloud app store studies
-  Microsoft Azure App Store
   - Collect data on [Microsoft Azure](https://azuremarketplace.microsoft.com/en-US/) applications
-  Amazon Web Services (AWS) Marketplace
   - Collect data on [AWS](https://azuremarketplace.microsoft.com/en-US/) applications

## New analysis processors
- Take screenshots of any column containing URLs
- Detect trackers
  - Provide a list of various source code to search for in collected HTML
 
# Installation
These extensions are designed to work with [4CAT v1.46](https://github.com/digitalmethodsinitiative/4cat/releases/tag/v1.46) or later.
## Docker installation
1. Download/clone extensions into both 4CAT backend and frontend containers
  - `docker exec 4cat_backend git clone https://github.com/digitalmethodsinitiative/4cat_web_studies_extensions.git extensions/web_studies/`
  - `docker exec 4cat_frontend git clone https://github.com/digitalmethodsinitiative/4cat_web_studies_extensions.git extensions/web_studies/`
2. Restart 4CAT containers
  - `docker compose restart` from 4CAT directory where `docker-compose.yml` and `.env` files were previously downloaded
  - This will automatically install necessary dependencies, Firefox, and Geckodriver
3. Activate desired new datasources from the 4CAT Control Panel
  - Control Panel -> Settings -> Data sources

## Direct/manual installation
1. Download or clone this repository and copy the folders into the `extensions` folder in your 4CAT directory
  - `git clone https://github.com/digitalmethodsinitiative/4cat_web_studies_extensions.git extensions/web_studies/`
2. Run 4CAT's migrate script to install necessary packages
  - `python helper-scripts/migrate.py`
  - Note: `fourcat_insall.py` is only designed to run on linux systems. For other systems you will need set up the following:
    - Install python packages from `requirements.txt`
    - Download Firefox
    - Download the appropriate Geckodriver compatible with that version of Firefox (https://github.com/mozilla/geckodriver/releases/)
    - Adjust settings in 4CAT interface via `Control Panel -> Settings -> selenium` to point to Firefox/Geckodriver programs
3. Activate desired datasources from the 4CAT Control Panel
  - Control Panel -> Settings -> Data sources
  

