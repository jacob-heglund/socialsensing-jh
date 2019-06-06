# -*- coding: utf-8 -*-
"""
Functions for testing twitterinfrastructure.clean_nyctlc module.


"""

import pandas as pd
from twitterinfrastructure import import_nyctlc as clean
from twitterinfrastructure.tools import query


# def test_dl_urls():


def test_import_trips():

    url_path = None
    dl_dir = 'tests/nyctlc/raw/'
    db_path = 'tests/nyctlc/test.db'
    taxi_type = 'yellow'
    usecols = ['vendor_id', 'pickup_datetime', 'dropoff_datetime',
               'passenger_count', 'trip_distance', 'pickup_longitude',
               'pickup_latitude', 'pickup_location_id', 'dropoff_longitude',
               'dropoff_latitude', 'dropoff_location_id']

    dl_num, import_num = clean.import_trips(url_path, dl_dir, db_path,
                                            taxi_type,
                                            nrows=None, usecols=usecols,
                                            overwrite=True, verbose=0)

    sql = 'SELECT * FROM trips;'
    df_test = query(db_path, sql)

    assert dl_num == 0 and import_num == 2 and \
        df_test['trip_id'][0] == 1 and \
        df_test['taxi_type'][0] == 2 and \
        df_test['vendor_id'][0] == 1 and \
        df_test['pickup_datetime'][0] == '2012-10-01 07:57:00' and \
        df_test['dropoff_datetime'][0] == '2012-10-01 07:57:00' and \
        df_test['passenger_count'][0] == 1 and \
        df_test['trip_distance'][0] == 0.2 and \
        round(df_test['pickup_longitude'][0], 2) == -73.98 and \
        round(df_test['pickup_latitude'][0], 2) == 40.79 and \
        pd.isnull(df_test['pickup_location_id'][0]) and \
        round(df_test['dropoff_longitude'][0], 2) == -73.98 and \
        round(df_test['dropoff_latitude'][0], 2) == 40.79 and \
        pd.isnull(df_test['dropoff_location_id'][0]) and \
        df_test['trip_duration'][0] == 0 and \
        df_test['trip_pace'][0] == 0 and \
        round(df_test['trip_straightline'][0], 2) == 0.28 and \
        round(df_test['trip_windingfactor'][0], 2) == 0.73
