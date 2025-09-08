This data source collects data from the Internet Archive's [Wayback Machine](https://web.archive.org/). For a given URL or list of URLs,
historical snapshots are retrieved and opened with an automated browser, [Firefox](https://www.firefox.com/). A screenshot of what the
snapshot looks like in the browser is then made and returned.

This can be used to visually trace the evolution of a site over time, by e.g. comparing yearly snapshots over a period of several years.

Note that the Wayback Machine is slow and its archive is incomplete. Datasets can take quite a while to complete and may not contain 
every screenshot asked for. It is often best to use this tool as a companion to manual data collection; browse the archive for a site
manually first, to get a sense of how complete it is and what kind of snapshot interval would make for interesting findings. 

For low numbers of snapshots, manual screenshotting may be faster. If your case study requires a large(r) number of screenshots, then 
automating the process with this data source may be helpful.
