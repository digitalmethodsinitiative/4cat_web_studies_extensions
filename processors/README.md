# Web Studies Processors
## Installation
### Detect trackers
The Detect trackers analysis requires access to the [Ghostery tracker database](https://github.com/ghostery/trackerdb?tab=readme-ov-file#ghostery-tracker-database). 4CAT includes an automated worker, `ghostery-data-collector`, which will run daily to update the database. This worker, however, requires `nodejs` and `npm` as well as `git` to download and update this database. It will attempt to install `nodejs` and `npm`, but it is possible that 4CAT does not have the proper authorization to do so.

#### Use our installation worker
If you see a warning telling you to manually install the Ghostery database, you can run the installation directly. You may need to use `sudo`:
- From 4CAT root (defaults to ./config for PATH_CONFIG):
    - `python3 -m externsions.web_studies.processors.install_ghostery`
- If you change the default config path in your `config.ini` file:
    - `python3 -m processors.install_ghostery --path-config /path/to/4cat/config`
- Running into more issues? You can use versbose:
    - `python3 -m processors.install_ghostery -v`

#### Additional Issues
If you are not on Linux or run into other issues, you can attempt to install manually
1. On a linux system, install `nodejs` and `npm`:
- `apt update && apt install -y nodejs npm`
- You may need to run the command with `sudo`
2. Clone the Ghostery tracker database repo `https://github.com/ghostery/trackerdb.git`. 4CAT may have already handled this as it should have `git` rights.
- This should be located in your 4CAT config directory: `config/ghostery`
- `git clone https://github.com/ghostery/trackerdb.git your/4cat/directory/config/ghostery`
3. Navigate to this directory and install the package
- `cd your/4cat/directory/config/ghostery`
- `npm install`
4. Build the database (i.e. a file called `trackerdb.json`)
- `node your/4cat/directory/config/ghostery/scripts/export-json/index.js`
5. You may need to restart 4CAT for changes to take effect and the analysis to become available

This should make Ghostery's tracker database available for the Detect trackers analysis. 4CAT will not know what version or when this was updated unless you also create a `.last_update.json` file in the ghostery folder, however, it will still use the new database. 4CAT will read this next time the update worker is run.
```
    payload = {
        "latest_release": "ghostery_latest_release",
        "checked_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "status": status (i.e.,"updated" | "up-to-date" | "failed"),
    }
```