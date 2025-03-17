# Web Studies Processors
## Installation
### Detect trackers
The Detect trackers analysis requires access to the [Ghostery tracker database](https://github.com/ghostery/trackerdb?tab=readme-ov-file#ghostery-tracker-database). 4CAT includes an automated worker, `ghostery-data-collector`, which will run periodically to updated the database. This worker, however, requires `nodejs` and `npm` as well as `git` to download and update this database. It will attempt to install `nodejs` and `npm`, but it is possible that 4CAT does not have the proper authorization to do so.

If you see a warning telling you to manually install the Ghostery database, you can follow the below instructions:

1. On a linux system, install `nodejs` and `npm`:
- `apt update && apt install -y nodejs npm`
- You may need to run the command with `sudo`
2. Close the Ghostery tracker database repo `https://github.com/ghostery/trackerdb.git`. 4CAT may have already handled this as it should have `git` rights.
- This should be located in your 4CAT config directory: `config/ghostery`
- `git clone https://github.com/ghostery/trackerdb.git your/4cat/directory/config/ghostery`
3. Navigate to this directory and install the package
- `cd your/4cat/directory/config/ghostery`
- `npm install`
4. Build the database (i.e. a file called `trackerdb.json`)
- `node your/4cat/directory/config/ghostery/scripts/export-json/index.js`
5. You may need to restart 4CAT for changes to take effect and the analysis to become available

This should make Ghostery's tracker database available for the Detect trackers analysis. 4CAT should be able to maintain and update the database so long as 4CAT is able to run `nodejs` and `npm`.