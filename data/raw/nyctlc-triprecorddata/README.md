# Data Overview

#### yellow
- 2009 has different columns from 2010-2013.
- 2010-2013 have the same column names.
- 2014 has the same column names as 2013, except with a space preceding some columns.
- 2015 and 2016 have the same columns names (which match columns specified in `data_dictionary_trip_records_yellow.pdf`).
- 2017 has the same column names as 2016, except with pickup/dropoff lat/lon replaced by pickup/dropoff location IDs (which are specified in `taxi+_zone_lookup.csv`).
- Some rows in 2010-02 and 2010-3 are shifted (empty in dropoff_longitude); this is why clean_yellow uses error_bad_lines=False.
- Last row of 2010-12 is missing values.
- Some rows have 0 for missing information (e.g. passenger_count and trip_distance for row 438798 of 2010-08).
- store_and_fwd_flag column
  - Values are [nan, 0, 1] for 2009 and 2010 (aside from 2010-01 and 2010-02)
  - 2010-01 and 2010-02 have '*' and ''
  - Values are [nan, 'Y', 'N'] for 2010-08 - ?
  - 2010-12 has [nan, 'Y', 'N', '0']

# Mapping
- [geofabrik link](http://download.geofabrik.de/north-america.html) for osm maps
- [qgis tutorial](https://multimedia.journalism.berkeley.edu/tutorials/qgis-basics-journalists/) from berkeley
- [link for checking if shapefile contains lat/lon](https://stackoverflow.com/questions/7861196/check-if-a-geopoint-with-latitude-and-longitude-is-within-a-shapefile)
- [link](http://gsp.humboldt.edu/OLM/Courses/GSP_318/07_3_AccessingAttributes.html) for handling attributes in python
- [link](https://macwright.org/2012/10/31/gis-with-python-shapely-fiona.html) for analyzing geospatial data in Python
- [link](http://toblerity.org/fiona/manual.html#record-properties) for fiona manual
- [link](https://stackoverflow.com/questions/31900600/python-and-shapefile-very-large-coordinates-after-importing-shapefile) for coordinate projections and transformations
