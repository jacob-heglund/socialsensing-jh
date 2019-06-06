# -*- coding: utf-8 -*-
"""
Functions for testing twitterinfrastructure.analyze_nyctlc module.


"""

from twitterinfrastructure import analyze_nyctlc as analyze
from twitterinfrastructure.tools import query
import unittest


class TestNyctlc(unittest.TestCase):
    db_path = 'tests/nyctlc/test.db'
    start_datetime = '2012-10-01 00:00:00'
    end_datetime = '2012-11-20 23:59:59'
    shapefile_path = 'data/processed/taxi_zones_wgs84/taxi_zones_wgs84.shp'
    taxizones_path = 'data/raw/nyctlc-triprecorddata/taxi+_zone_lookup.csv'
    title = 'test'
    trips_analysis_table = 'trips_analysis_test'
    taxi_zones_table = 'taxi_zones_test'

    #tests for summary route time
    def test_grouping_borough_day(self):

        # test grouping by borough and day
        analyze.create_summary_route_time(self.db_path, byborough=True, byday=True,
                                          title=self.title,
                                          trips_analysis_table=self.trips_analysis_table,
                                          taxi_zones_table=self.taxi_zones_table,
                                          overwrite=True, verbose=0)
        sql = 'SELECT * FROM summary_routeborough_day_{title};'.format(title=self.title)
        df_test = query(self.db_path, sql, parse_dates=False, verbose=0)
        assert list(df_test['pickup_borough_id']) == [4, 4] and \
               list(df_test['dropoff_borough_id']) == [4, 4] and \
               list(df_test['trip_count']) == [1, 5] and \
               [round(pace, 2) for pace in df_test['mean_pace']] == [183.21, 324.02]

    def test_gouping_borough_hour(self):
        # test grouping by borough and hour
        analyze.create_summary_route_time(self.db_path, byborough=True, byday=False,
                                          title=self.title,
                                          trips_analysis_table=self.trips_analysis_table,
                                          taxi_zones_table=self.taxi_zones_table,
                                          overwrite=True, verbose=0)
        sql = 'SELECT * FROM summary_routeborough_hour_{title};'.format(title=self.title)
        df_test = query(self.db_path, sql, parse_dates=False, verbose=0)
        assert list(df_test['pickup_borough_id']) == [4, 4, 4] and \
               list(df_test['dropoff_borough_id']) == [4, 4, 4] and \
               list(df_test['trip_count']) == [1, 3, 2] and \
               [round(pace, 2) for pace in df_test['mean_pace']] == [183.21, 285.71,
                                                                     400.00]

    def test_grouping_zone_day(self):
        # test grouping by zone and day
        analyze.create_summary_route_time(self.db_path, byborough=False, byday=True,
                                          title=self.title,
                                          trips_analysis_table=self.trips_analysis_table,
                                          taxi_zones_table=self.taxi_zones_table,
                                          overwrite=True, verbose=0)
        sql = 'SELECT * FROM summary_routezone_day_{title};'.format(title=self.title)
        df_test = query(self.db_path, sql, parse_dates=False, verbose=0)
        assert list(df_test['pickup_location_id']) == [249, 90, 162] and \
               list(df_test['dropoff_location_id']) == [90, 161, 229] and \
               list(df_test['trip_count']) == [1, 1, 4] and \
               [round(pace, 2) for pace in df_test['mean_pace']] == [183.21, 303.37,
                                                                     334.26]

    def test_grouping_zone_hour(self):
        # test grouping by zone and hour
        analyze.create_summary_route_time(self.db_path, byborough=False, byday=False,
                                          title=self.title,
                                          trips_analysis_table=self.trips_analysis_table,
                                          taxi_zones_table=self.taxi_zones_table,
                                          overwrite=True, verbose=0)
        sql = 'SELECT * FROM summary_routezone_hour_{title};'.format(title=self.title)
        df_test = query(self.db_path, sql, parse_dates=False, verbose=0)
        assert list(df_test['pickup_location_id']) == [249, 90, 162, 162] and \
               list(df_test['dropoff_location_id']) == [90, 161, 229, 229] and \
               list(df_test['trip_count']) == [1, 1, 2, 2]

    def test_create_trips_analysis(self):
        analyze.create_trips_analysis(self.db_path, self.start_datetime, self.end_datetime,
                                      self.shapefile_path, self.taxizones_path, title='test',
                                      overwrite=True, create_zones_tables=True,
                                      verbose=0)

        sql_trips = 'SELECT * FROM trips_analysis_test;'
        df_test = query(self.db_path, sql_trips)
        sql_taxi_zones = 'SELECT * FROM taxi_zones_test;'
        df_zones = query(self.db_path, sql_taxi_zones)
        sql_taxi_boroughs = 'SELECT * FROM taxi_boroughs_test;'
        df_boroughs = query(self.db_path, sql_taxi_boroughs)

        assert list(df_test['trip_id']) == [4, 5, 6, 7, 8, 9] and \
               list(df_test['pickup_location_id']) == [249, 90, 162, 162, 162,
                                                       162] and \
               list(df_test['dropoff_location_id']) == [90, 161, 229, 229, 229,
                                                        229] and \
               list(df_test['pickup_hour']) == [7, 15, 15, 15, 16, 16]

        assert df_zones.shape == (265, 4)

        assert list(df_boroughs['borough_id']) == [1, 2, 3, 4, 5, 6] and \
               list(df_boroughs['borough_name']) == ['Bronx', 'Brooklyn', 'EWR',
                                                     'Manhattan', 'Queens',
                                                     'Staten Island']
