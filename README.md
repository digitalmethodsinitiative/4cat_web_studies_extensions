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
1. Currently 4CAT will need to use the [`extensions`](https://github.com/digitalmethodsinitiative/4cat/tree/extensions) branch until changes are released in a future version
 - Enable "Can upgrade to development branch" under "User priviledges" in the Control Panel
 - Under "Restart or Upgrade", set branch to `extensions` and click "Update to branch"
2. Download or clone this repository and copy the folders into the `extensions` folder in your 4CAT directory
  - `git clone https://github.com/digitalmethodsinitiative/4cat_web_studies_extensions.git temp/`
  - `mv temp/* extensions/`
  - Note: with Docker you will need to do this for both backend and frontend containers (e.g., by first connecting to the containers via `docker exec -it 4cat_backend bash` or `docker exec -it 4cat_frontend bash`)

