# Twitter Infrastructure
## General Overview
* Hurricane Data
    - twitter\_sandy.py
* Power Data
    - nyiso.py
* Taxi Data
    - analysze\_nyctcl.py
    - import\_nyctlc.py



## Helper Functions
### tools.py
* boxcox\_backtransform
* check\_expected\_list
* connect\_db
* create\_table
* cross\_corr
* df\_to\_table
* dump
* get\_regex\_files
* haversine
* output
* read\_shapefile
* query

## Database
### twitter\_sandy.py
* create\_analysis
* create\_tweets\_keyword
* insert\_tweets
* mongod\_to\_df
* query\_groupby
* query\_groupby\_norm
* query\_groupby\_hour
* query\_groupby\_hour\_norm
* query\_keyword

### nyiso.py
* create\_expected\_load
* create\_forecase\_err
* create\_standard\_load
* import\_load
* import\_load\_forecast

### import\_nyctlc.py
* add\_trip\_column
* clean\_column\_names
* clean\_datetime
* clean\_lat\_lon
* clean\_payment\_type
* clean\_store\_and\_fwd\_flag
* clean\_vendor\_id
* clean\_yellow 
* col\_names\_dict
* dl\_urls
* import\_trips
* load\_yellow
* taxi\_regex\_patterns



## Data Analysis
### twitter\_sandy.py
* create\_hydrator\_tweetids
* process\_heat\_map\_daily
* tokenize\_tweet

### nyiso.py
* clean\_isolf
* clean\_palent
* load\_loaddate

### analyze\_nyctlc.py
* add\_date\_hour
* add\_location\_id
* create\_expected\_zone\_data
* create\_expected\_zone\_hour
* create\_standard\_zone\_data
* create\_standard\_zone\_hour
* create\_summary\_route\_time
* create\_summary\_zone
* create\_summary\_zone\_time
* create\_taxi\_zones
* create\_trips\_analysis
* points\_in\_shapefile
* process\_head\_map\_daily 
* query\_trips\_filtered

### analysis.py
* create\_timeseries\_diff
* create\_timeseries\_shift
* index\_timedelta
* load\_nytlc\_zone
* load\_nytlc\_zone\_date
* load\_nytlc\_zone\_date
* load\_nyiso
* max\_cross\_corr
* plot\_acf\_series
* plot\_timeseries

