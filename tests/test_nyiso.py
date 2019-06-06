# -*- coding: utf-8 -*-
"""
Functions for testing twitterinfrastructure.nyiso module.


"""

import pandas as pd
from twitterinfrastructure import nyiso as ny
from twitterinfrastructure.tools import query
import unittest


class TestNYISO(unittest.TestCase):
    db_path = 'tests/nyiso/test.db'
    dl_dir = 'tests/nyiso/raw/'
    zones_path = 'tests/nyiso/raw/nyiso-zones.csv'

    def test_clean_isolf(self):
        load_type = 'isolf'
        df = ny.load_loaddate('20121030', load_type=load_type, dl_dir=self.dl_dir)
        df = ny.clean_isolf(df, to_zoneid=True, zones_path=self.zones_path, verbose=0)

        assert (df.loc[('2012-10-30 01:00:00', 2), ['load_forecast_p0']].values[0][0]
                == 860) and (df.shape == (1584, 7))

    def test_clean_palint(self):
        load_type = 'palIntegrated'
        df = ny.load_loaddate('20121030', load_type=load_type, dl_dir=self.dl_dir)
        df = ny.clean_palint(df, to_zoneid=True, zones_path=self.zones_path, verbose=0)
        print(df)

        assert (df.loc[('2012-10-30 06:00:00', 2), ['integrated_load']].values[0][0]
                == 824.9) and (df.shape == (264, 1))

    def test_create_expected_load(self):
        summary_table = 'load'
        start_ref = pd.Timestamp('2012-10-01 00:00:00', tz='America/New_York')
        end_ref = pd.Timestamp('2012-10-20 23:59:59', tz='America/New_York')
        datetimeUTC_range_ref = (start_ref.tz_convert(tz='UTC').tz_localize(None),
                                 end_ref.tz_convert(tz='UTC').tz_localize(None))
        start = pd.Timestamp('2012-10-01 00:00:00', tz='America/New_York')
        end = pd.Timestamp('2012-10-05 23:59:59', tz='America/New_York')
        datetimeUTC_range = (start.tz_convert(tz='UTC').tz_localize(None),
                             end.tz_convert(tz='UTC').tz_localize(None))
        _ = ny.create_expected_load(self.db_path, summary_table, self.zones_path,
                                    datetimeUTC_range_ref,
                                    datetimeUTC_range_excl=datetimeUTC_range,
                                    title='test', overwrite=True, verbose=0)

        sql = 'SELECT * FROM expected_load_test;'
        df_test = query(self.db_path, sql)

        assert (df_test.loc[2, ['mean_integrated_load']].values[0] == 1385) and \
               (df_test.loc[2, ['num_rows']].values[0] == 2) and \
               (df_test.shape == (1848, 7))

    def test_create_standard_load(self):
        summary_table = 'load'
        expected_table = 'expected_load_test'
        start = pd.Timestamp('2012-10-01 00:00:00', tz='America/New_York')
        end = pd.Timestamp('2012-10-05 23:59:59', tz='America/New_York')
        datetimeUTC_range = (start.tz_convert(tz='UTC').tz_localize(None),
                             end.tz_convert(tz='UTC').tz_localize(None))
        _ = ny.create_standard_load(self.db_path, summary_table, expected_table,
                                    datetimeUTC_range, min_num_rows=1,
                                    title='test', overwrite=True, verbose=0)

        sql = 'SELECT * FROM standard_load_test;'
        df_test = query(self.db_path, sql)

        assert (round(df_test.loc[2, ['z_integrated_load']].values[0], 2) ==
                -0.04) and (df_test.shape == (1320, 4))

    def test_import_load(self):
        import_num = ny.import_load(self.dl_dir, self.db_path, to_zoneid=True,
                                    zones_path=self.zones_path, overwrite=True,
                                    verbose=0)
        sql = 'SELECT * FROM load;'
        df_test = query(self.db_path, sql)

        assert (import_num == 31) and (df_test.loc[5, 'rowid'] == 6) and \
               (df_test.loc[5, 'datetimeUTC'] == '2012-10-01 04:00:00') and \
               (df_test.loc[5, 'zone_id'] == 6) and \
               (df_test.loc[5, 'integrated_load'] == 1015.9) and \
               (df_test.shape == (8184, 4))

    def test_load_loaddate(self):
        load_type_integrated = 'palIntegrated'
        df = ny.load_loaddate('20121030', load_type=load_type_integrated, dl_dir=self.dl_dir)

        assert (df.loc[10, 'Time Stamp'] == '10/30/2012 00:00:00') and \
               (df.loc[10, 'Name'] == 'WEST') and \
               (df.loc[10, 'Integrated Load'] == 1586.2) and df.shape == (264, 5)

        load_type_isolf = 'isolf'
        df = ny.load_loaddate('20121030', load_type=load_type_isolf, dl_dir=self.dl_dir)

        assert (df.loc[4, 'Time Stamp'] == '10/30/2012 04:00') and \
               (df.loc[4, 'N.Y.C.'] == 4149) and df.shape == (144, 13)


