# Raw Data Overview

Details may be in README files within individual directories.

`FOIL2012-Donovan2016` - This dataset contains NYC taxi traffic data, based on a FOIL request from Donovan and Work. The data is from [here](https://databank.illinois.edu/datasets/IDB-9610843) and appears to be a duplicate of the `nyctlc-triprecorddata` without fare data (based on comparing data frame shapes and records for min/max pickup_datetime; see `nyctlc-comparison.ipynb`). We will use the `nyctlc-triprecorddata` data instead, since it contains more years and fare data.

`gifhistory2.txt` - This Tweet ID dataset is used to test the data format required for Hydrator. The data is from [here](https://dash.ucr.edu/stash/dataset/doi:10.6086/D1CM12).

`harvey_twitter_dataset.tar` - This dataset contains Tweet IDs from ...

`journal.pone.0167267.s006-Guan2016.csv` - This dataset contains NYC subway ridership from October 1 to November 30 in 2011 and 2012. The data is from [Guan et al. 2016](http://journals.plos.org/plosone/article?id=10.1371/journal.pone.0167267#sec014).

`Link Level Traffic Estimates-Donovan` - This dataset contains NYC taxi traffic estimates from Donovan and Work. The data is from [here](https://uofi.app.box.com/v/NYC-traffic-estimates) with a description and other related data found [here](https://my.vanderbilt.edu/danwork/open-data-2/).

`nyctlc-triprecorddata` - This directory contains NYC taxi trip data. The data is from [here](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml).

`release-mdredze-short.txt` - This file contains the first 10 Tweet IDs from `release-mdredze.txt` and is used to test the clean_sandy function.

`release-mdredze.txt` - This file contains all Tweet IDs from the period surrounding Hurricane Sandy (2012-10-22 to 2012-11-02, geotagged within Washington DC, CT, DE, MA, MD, NJ, NY, NC, OH, PA, RI, SC, VA, or WV). The data is from [here](https://github.com/mdredze/twitter_sandy) with details described in [Wang et al. 2015](https://www.aaai.org/ocs/index.php/WS/AAAIW15/paper/download/10079/10258).

`Taxi_Trips.csv` - This dataset contains Chicago taxi trip data. The data (with a description) is from [here](https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew).

# Interim Data Overview

`nyctlc-triprecorddata` - This directory contains interim data used for importing and analyzing NYC taxi trip data.

`sandy-tweetids-short.txt` - This file contains tweet ids from `raw/release-mdredze-short.txt`, formatted for hydration using Hydrator.

`sandy-tweetids.txt` - This file contains tweet ids from `raw/release-mdredze.txt`, formatted for hydration using Hydrator.

# Processed Data Overview

`nyctlc-triprecorddata.db` - This is a sqlite database containing imported data from the `raw/nyctlc-triprecorddata/` directory.

`sandy-tweets-short-[date].json` - This file contains tweets hydrated using Hydrator from `interim/sandy-tweets-short.txt` on [date].

`sandy-tweets-[date].json` - This file contains tweets hydrated using Hydrator from `interim/sandy-tweets.txt` on [date].

# Other Possible Data Sources
- This [link](http://www.ercot.com/gridinfo/load/load_hist) contains power load data by zone and hour for ERCT (i.e. Texas).
- This [link](http://fema.maps.arcgis.com/home/item.html?id=307dd522499d4a44a33d7296a5da5ea0) contains FEMA MOTF Sandy analysis data and links. Some of this data was used by [Guan and Chen 2014](http://link.springer.com/10.1007/s11069-014-1217-1) and [Kryvasheyeu et al. 2016](http://advances.sciencemag.org/cgi/doi/10.1126/sciadv.1500779).
- This [link](https://github.com/tmrowco/electricitymap#real-time-electricity-data-sources) references a GitHub project on electricity maps, with some past issues discussing ISO shapefiles.
- This [link](https://www.eia.gov/maps/layer_info-m.php) contains shapefile maps available from EIA.
- This [link](https://www.arcgis.com/home/item.html?id=3a510da542c74537b268657f63dc2ce4#overview) contains an ArgGIS feature service with NYISO load zones mapped out.
- This [link](https://univofillinois.maps.arcgis.com/home/item.html?id=6fd1de467b134f47a607721f23a69f0c) contains a NYISO load zones map.

# Data Processing Links
- Short read discussing MongoDB with examples [here](http://stats.seandolinar.com/collecting-twitter-data-storing-tweets-in-mongodb/).
- Stackoverflow post using mongodump and mongorestore to transfer a MongoDB database [here](https://stackoverflow.com/questions/11255630/how-to-export-all-collection-in-mongodb).
- Intro to tweet json from Twitter [here](https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/intro-to-tweet-json.html).
- Twitter geo-objects [overview](https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/geo-objects).
- Example of geospatial queries with MongoDB [here](https://docs.mongodb.com/manual/tutorial/geospatial-tutorial/).
- Discussion of time series analysis with Pandas [here](https://jakevdp.github.io/PythonDataScienceHandbook/03.11-working-with-time-series.html).

# Mapping Links
- Berkely journalism tutorial for using QGIS [here](https://multimedia.journalism.berkeley.edu/tutorials/qgis-basics-journalists/) and a [guide](http://www.qgistutorials.com/en/docs/making_a_map.html) for creating QGIS maps (with legend).
- OSM maps from geofabrik [here](http://download.geofabrik.de/north-america.html).
- Stackoverflow post for checking if a shapefile contains a  lat/lon geopoint [here](https://stackoverflow.com/questions/7861196/check-if-a-geopoint-with-latitude-and-longitude-is-within-a-shapefile).
- Notes for handling shapefile attributes in Python [here](http://gsp.humboldt.edu/OLM/Courses/GSP_318/07_3_AccessingAttributes.html).
- Link for analyzing GIS geospatial data in Python with Shapefly and Fiona  [here](https://macwright.org/2012/10/31/gis-with-python-shapely-fiona.html).
- Link to Fiona manual [here](http://toblerity.org/fiona/manual.html#record-properties).
- Stackoverflow post on coordinate projections and transformations [here](https://stackoverflow.com/questions/31900600/python-and-shapefile-very-large-coordinates-after-importing-shapefile).
- Support for exporting an ArcGIS feature service to another file format [here](https://support.esri.com/en/technical-article/000012638).

#Required Offline Databases

- nyiso-2012.db in data/processed/
- nyctlc-2012.db in data/processed/