# -*- coding: utf-8 -*-
"""
Functions for importing nyiso data.


"""

import calendar
import numpy as np
import pandas as pd
import re
import zipfile
from twitterinfrastructure.tools import connect_db, create_table, df_to_table, \
    get_regex_files, output, query


def clean_isolf(df, to_zoneid=False, zones_path=None, verbose=0):
    """Cleans a dataframe of nyiso load forecast data. Cleaning involves:
    renaming columns, converting datetimes, setting indexes, removing
    columns, converting to zone_id, and reshaping.

    Parameters
    ----------
    df : dataframe
        Dataframe to clean.

    to_zoneid : bool
        If True, converts zone names to zone ids, based on zones_path csv
        (zones_path must be defined if True). If False, leaves zones_name
        column.

    zones_path : str or None
        Path to csv mapping zone_id to zone_name. Required if to_zoneid is True.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe
        Cleaned dataframe.

    Notes
    -----
    """

    if verbose >= 2:
        output('Started cleaning dataframe.')

    # clean column names
    df = df.rename(columns={'Time Stamp': 'datetimeNY',
                            'Capitl': 'CAPITL',
                            'Centrl': 'CENTRL',
                            'Dunwod': 'DUNWOD',
                            'Genese': 'GENESE',
                            'Hud Vl': 'HUD VL',
                            'Longil': 'LONGIL',
                            'Mhk Vl': 'MHK VL',
                            'Millwd': 'MILLWD',
                            'N.Y.C.': 'N.Y.C.',
                            'North': 'NORTH',
                            'West': 'WEST'})

    # clean datetime
    df['datetimeNY'] = pd.to_datetime(df['datetimeNY'], format='%m/%d/%Y %H:%M')

    if any(df.duplicated('datetimeNY')):
        # deal with ambiguous time zone due to end of DST (two 01:00 entries)
        transition_idx = next(
            i for i, val in enumerate(df.duplicated('datetimeNY'))
            if val)
        datetimes = []
        for i, val in enumerate(df['datetimeNY']):
            if i < transition_idx:
                datetimes.append(val.tz_localize(tz='America/New_York',
                                                 ambiguous=True))
            else:
                datetimes.append(val.tz_localize(tz='America/New_York',
                                                 ambiguous=False))
        df['datetimeNY'] = datetimes
    else:
        df['datetimeNY'] = [datetime.tz_localize(tz='America/New_York') for
                            datetime in df['datetimeNY']]

    # set index
    df = df.set_index('datetimeNY')

    # remove columns
    df = df.drop(['NYISO'], axis=1)

    # clean zone_id
    if to_zoneid:
        zone_col = 'zone_id'
        if zones_path:
            df_zones = pd.read_csv(zones_path)
            zones = dict(zip(df_zones['name'], df_zones['zone_id']))
            df = df.rename(columns=zones)
        else:
            raise ValueError('Must provide zones_path argument if to_zoneid is '
                             'True.')
    else:
        zone_col = 'zone_name'

    # reshape dataframe
    s = df.stack()
    s.index.names = ['datetimeNY', zone_col]
    df = pd.DataFrame(s.rename('load_forecast'))
    df = df.sort_index(level=0)
    dates = df.index.get_level_values(0).date
    for i, date in enumerate(pd.unique(dates)):
        print(date)
        col_name = 'load_forecast_p' + str(i)
        df[col_name] = np.nan
        s = df[dates == date]['load_forecast'].copy()
        s = s.rename(col_name)
        df.update(s)
    df = df.drop('load_forecast', axis=1)

    # add utc column
    datetimeUTC = [datetime.tz_convert('UTC') for datetime in
                   df.index.get_level_values(0)]
    df.insert(0, 'datetimeUTC', datetimeUTC)

    if verbose >= 2:
        output('Finished cleaning dataframe.')

    return df


def clean_palint(df, to_zoneid=False, zones_path=None, verbose=0):
    """Cleans a dataframe of nyiso integrated real-time actual load data.
    Cleaning involves: renaming columns, converting datetimes (assumes ny
    timezone), converting to zone_id, removing columns, and setting indexes.

    Parameters
    ----------
    df : dataframe
        Dataframe to clean.

    to_zoneid : bool
        If True, converts zone names to zone ids, based on zones_path csv
        (zones_path must be defined if True). If False, leaves zones_name
        column.

    zones_path : str or None
        Path to csv mapping zone_id to zone_name. Required if to_zoneid is True.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe
        Cleaned dataframe.

    Notes
    -----
    """

    if verbose >= 2:
        output('Started cleaning dataframe.')

    # clean column names
    df = df.rename(columns={'Time Stamp': 'datetime',
                            'Time Zone': 'timezone',
                            'Name': 'name',
                            'Integrated Load': 'integrated_load'})

    # clean datetime
    df['datetime'] = pd.to_datetime(df['datetime'], format='%m/%d/%Y %H:%M:%S')
    offset = df['timezone'].replace({'EDT': pd.Timedelta('4 hours'),
                                     'EST': pd.Timedelta('5 hours')})
    df['datetimeUTC'] = offset + pd.to_datetime(df['datetime'],
                                                format='%m/%d/%Y %H:%M:%S')
    df['datetimeUTC'] = [dtUTC.tz_localize(tz='UTC') for dtUTC in
                         df['datetimeUTC']]

    # clean zone_id
    if to_zoneid:
        zone_col = 'zone_id'
        if zones_path:
            df_zones = pd.read_csv(zones_path)
            zones = dict(zip(df_zones['name'], df_zones['zone_id']))
            print(df.keys())
            df['zone_id'] = df['name'].replace(zones)
        else:
            raise ValueError('Must provide zones_path argument if to_zoneid is '
                             'True.')
    else:
        zone_col = 'zone_name'

    # remove columns
    df = df[['datetimeUTC', zone_col, 'integrated_load']]

    # set index
    df = df.set_index(['datetimeUTC', zone_col])
    df = df.sort_index(level=0)

    if verbose >= 2:
        output('Finished cleaning dataframe.')

    return df


def create_expected_load(db_path, summary_table, zones_path,
                         datetimeUTC_range_ref, datetimeUTC_range_excl=None,
                         title=None, overwrite=False, verbose=0):
    """Creates a table and dataframe of expected data from the summary_table
    table. Expectation includes mean and variance of integrated_load for the
    specified reference datetime range. Expectation is calculated for every
    possible dayofweek-hour-zone combination, with NaNs for those missing data.

    Parameters
    ----------
    db_path : str
        Path to sqlite database to create or connect to.

    summary_table : str
        Name of the db summary table containing data to calculate
        expected integrated_load from.

    zones_path : str
        Path to csv containing all zone_id values (maps zone_id to zone_name).

    datetimeUTC_range_ref : tuple
        Specifies the start and end of the reference time period to use when
        calculating expected values (inclusive). Specify as a 2-element
        tuple of UTC datetime strings with year-month-day and
        hour:minutes:seconds.

    datetimeUTC_range_excl : tuple
        Specifies the start and end of time period to exclude from reference
        time period. Specify as a 2-element tuple of UTC datetime strings with
        year-month-day and hour:minutes:seconds.

    title : str
        Defines the suffix of the expected_load_[title] table to be created.

    overwrite : bool
        Defines whether or not to overwrite existing table.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df_exp : dataframe
        Dataframe written to db table.

    Notes
    -----
    datetimeUTC_range_ref items should be UTC, but with naize format (since
    sqlite does not handle time zones). For example, use the following to
    select reference data for Jan. 1 - Dec. 31 2012 (Eastern):
    start = pd.Timestamp('2012-01-01 00:00:00', tz='America/New_York')
    end = pd.Timestamp('2012-12-31 23:59:59', tz='America/New_York')
    datetimeUTC_range_ref = (start.tz_convert(tz='UTC').tz_localize(None),
                            end.tz_convert(tz='UTC').tz_localize(None))
    """

    table = 'expected_load_{title}'.format(title=title)
    if verbose >= 1:
        output('Started creating or updating {table} table.'.format(
            table=table))

    # query range of zone_id values to consider
    df_zones = pd.read_csv(zones_path)
    zones = df_zones['zone_id'].unique()
    del df_zones

    # query reference data
    if datetimeUTC_range_excl:
        sql = """
            SELECT datetimeUTC, zone_id, integrated_load
            FROM {summary_table}
            WHERE
                (datetimeUTC BETWEEN "{start_datetime}" AND "{end_datetime}")
                AND (datetimeUTC NOT BETWEEN "{start_datetime_excl}" AND 
                    "{end_datetime_excl}")
        ;""".format(summary_table=summary_table,
                    start_datetime=datetimeUTC_range_ref[0],
                    end_datetime=datetimeUTC_range_ref[1],
                    start_datetime_excl=datetimeUTC_range_excl[0],
                    end_datetime_excl=datetimeUTC_range_excl[1])
    else:
        sql = """
            SELECT datetimeUTC, zone_id, integrated_load
            FROM {summary_table}
            WHERE
                (datetimeUTC BETWEEN "{start_datetime}" AND "{end_datetime}")
        ;""".format(summary_table=summary_table,
                    start_datetime=datetimeUTC_range_ref[0],
                    end_datetime=datetimeUTC_range_ref[1])
    df = query(db_path, sql)

    # add dayofweek (0 = Monday) and hour (0-23)
    df['datetimeUTC'] = pd.to_datetime(df['datetimeUTC'])
    df['datetimeUTC'] = [dtUTC.tz_localize(tz='UTC') for dtUTC in
                         df['datetimeUTC']]
    df['datetime'] = [dtUTC.tz_convert(tz='America/New_York') for dtUTC in
                      df['datetimeUTC']]

    df['dayofweek'] = df['datetime'].dt.dayofweek
    df['hour'] = df['datetime'].dt.hour

    # calculate mean and variance for each dayofweek-hour-zone combination
    expected = []
    for dayofweek in range(7):
        for hour in range(24):
            for zone in zones:
                # filter to current dayofweek, hour, and zone
                df_filter = df[(df['dayofweek'] == dayofweek) &
                               (df['hour'] == hour) &
                               (df['zone_id'] == zone)]

                # calculate mean and variance
                if not df_filter.empty:
                    mean_integrated_load = np.mean(
                        df_filter['integrated_load'].values)
                    var_integrated_load = np.var(
                        df_filter['integrated_load'].values)
                    num_rows = df_filter.shape[0]
                    expected.append([dayofweek, hour, zone,
                                     mean_integrated_load, var_integrated_load,
                                     num_rows])
                else:
                    expected.append([dayofweek, hour, zone,
                                     np.nan, np.nan, np.nan])
    df_exp = pd.DataFrame(expected,
                          columns=['dayofweek', 'hour', 'zone_id',
                                   'mean_integrated_load',
                                   'var_integrated_load', 'num_rows'])
    df_exp.set_index(['dayofweek', 'hour', 'zone_id'])

    # create table
    sql = """
            CREATE TABLE IF NOT EXISTS {table} (
                rowid INTEGER PRIMARY KEY,
                dayofweek INTEGER,
                hour INTEGER,
                zone_id INTEGER,
                mean_integrated_load FLOAT,
                var_integrated_load FLOAT,
                num_rows INTEGER
            ); """.format(table=table)
    create_table(db_path=db_path, table=table, create_sql=sql, indexes=[],
                 overwrite=overwrite, verbose=verbose)

    # write data to table
    df_to_table(db_path, df_exp, table=table, overwrite=False,
                verbose=verbose)

    if verbose >= 1:
        output('Finished creating or updating {table} table. Dataframe shape '
               'is '.format(table=table) + str(df_exp.shape) + '.')

    return df_exp


def create_forecast_err(db_path, load_table, forecast_table, overwrite=False,
                        verbose=0):
    """Creates a table and dataframe of load forecast error. Error is
    calculated as percent error relative to the actual load.

    I.e. error = (forecast - actual) / actual

    Parameters
    ----------
    db_path : str
        Path to sqlite database to create or connect to.

    load_table : str
        Name of the db table containing actual load data (i.e.
        based on palIntegrated data).

    forecast_table : str
        Name of the db table containing load forecast data (i.e. based on
        isolf).

    overwrite : bool
        Defines whether or not to overwrite existing table.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe
        Dataframe written to db table.

    Notes
    -----
    """

    if verbose >= 1:
        output('Started creating or updating forecast_error table.')

    # query actual loads
    sql = """
            SELECT datetimeUTC, zone_id, integrated_load
            FROM {load_table}
          ;""".format(load_table=load_table)
    df_load = query(db_path, sql)
    df_load['datetimeUTC'] = pd.to_datetime(df_load['datetimeUTC'])
    df_load = df_load.set_index(['datetimeUTC', 'zone_id'])

    # query forecast loads
    sql = """
            SELECT datetimeUTC, zone_id, load_forecast_p0, load_forecast_p1,
                load_forecast_p2, load_forecast_p3, load_forecast_p4, 
                load_forecast_p5, load_forecast_p6
            FROM {forecast_table}
          ;""".format(forecast_table=forecast_table)
    df_forecast = query(db_path, sql)
    df_forecast['datetimeUTC'] = pd.to_datetime(df_forecast['datetimeUTC'])
    df_forecast = df_forecast.set_index(['datetimeUTC', 'zone_id'])

    # calculate relative forecast errors
    df = pd.merge(df_load, df_forecast, how='inner', left_index=True,
                  right_index=True)
    del df_load, df_forecast
    df['forecast_error_p0'] = (df['load_forecast_p0'] -
                               df['integrated_load']) / df['integrated_load']
    df['forecast_error_p1'] = (df['load_forecast_p1'] -
                               df['integrated_load']) / df['integrated_load']
    df['forecast_error_p2'] = (df['load_forecast_p2'] -
                               df['integrated_load']) / df['integrated_load']
    df['forecast_error_p3'] = (df['load_forecast_p3'] -
                               df['integrated_load']) / df['integrated_load']
    df['forecast_error_p4'] = (df['load_forecast_p4'] -
                               df['integrated_load']) / df['integrated_load']
    df['forecast_error_p5'] = (df['load_forecast_p5'] -
                               df['integrated_load']) / df['integrated_load']
    df['forecast_error_p6'] = (df['load_forecast_p6'] -
                               df['integrated_load']) / df['integrated_load']
    df = df.drop(['load_forecast_p0', 'load_forecast_p1',
                  'load_forecast_p2', 'load_forecast_p3',
                  'load_forecast_p4', 'load_forecast_p5',
                  'load_forecast_p6'], axis=1)

    # create table
    sql = """
            CREATE TABLE IF NOT EXISTS forecast_error (
                rowid INTEGER PRIMARY KEY,
                datetimeUTC TEXT,
                zone_id INTEGER,
                integrated_load REAL,
                forecast_error_p0 REAL,
                forecast_error_p1 REAL,
                forecast_error_p2 REAL,
                forecast_error_p3 REAL,
                forecast_error_p4 REAL,
                forecast_error_p5 REAL,
                forecast_error_p6 REAL
          ); """
    indexes = ['CREATE UNIQUE INDEX IF NOT EXISTS '
               'forecast_error_datetimeUTC_zone_id ON forecast_error '
               '(datetimeUTC, zone_id);'
               ]
    create_table(db_path=db_path, table='forecast_error', create_sql=sql,
                 indexes=indexes,
                 overwrite=overwrite, verbose=verbose)

    # write data to table
    df_write = df.reset_index()
    df_write['datetimeUTC'] = df_write['datetimeUTC'].dt.tz_localize(
        None)
    df_to_table(db_path, df_write, table='forecast_error', overwrite=False,
                verbose=verbose)

    if verbose >= 1:
        output('Finished creating or updating forecast_error table. Dataframe '
               'shape is ' + str(df.shape) + '.')
    return df


def create_standard_load(db_path, summary_table, expected_table,
                         datetimeUTC_range, min_num_rows=5, title=None,
                         overwrite=False, verbose=0):
    """Creates a table and dataframe of standardized data from the
    summary_table table. Standardization is relative to the mean and variance of
    corresponding data from the specified reference datetime range (saved as
    an expected_load_[] table in the database).

    Parameters
    ----------
    db_path : str
        Path to sqlite database to create or connect to.

    summary_table : str
        Name of the db table containing summary data to calculate
        standardized integrated_load for.

    expected_table : str
        Name of the db table containing expected data (i.e. mean and
        variance) to calculate standardized integrated_load from.

    datetimeUTC_range : tuple
        Specifies the start and end of the time period to calculate
        standardized integrated_load for (inclusive). Specify as a 2-element
        tuple of UTC datetime strings with year-month-day and
        hour:minutes:seconds. E.g. ('2012-10-29 00:00:00', '2012-11-03
        23:59:59') to calculate standardized integrated_load for times between
        10/29/2012 and 11/03/2012.

    min_num_rows : int
        Defines the minimum number of rows needed in the reference set to
        standardize data.

    title : str
        Defines the suffix of the standard_load_[title] table to be created.

    overwrite : bool
        Defines whether or not to overwrite existing table.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df_std : dataframe
        Dataframe written to db table.

    Notes
    -----
    """

    table = 'standard_load_{title}'.format(title=title)
    if verbose >= 1:
        output('Started creating or updating {table} table.'.format(
            table=table))

    # query expected values calculated from at least min_num_rows data points
    sql = """
            SELECT * FROM {expected_table} 
            WHERE num_rows >= {min_num_rows};""".format(
        expected_table=expected_table, min_num_rows=min_num_rows)
    df_exp = query(db_path, sql)
    df_exp = df_exp[['dayofweek', 'hour', 'zone_id', 'mean_integrated_load',
                     'var_integrated_load']]

    # query data to standardize
    sql = """
            SELECT datetimeUTC, zone_id, integrated_load
            FROM {summary_table}
            WHERE
                datetimeUTC BETWEEN "{start_datetime}" AND "{end_datetime}";
            """.format(summary_table=summary_table,
                       start_datetime=datetimeUTC_range[0],
                       end_datetime=datetimeUTC_range[1])
    df = query(db_path, sql)

    # add dayofweek (0 = Monday) and hour (0-23)
    df['datetimeUTC'] = pd.to_datetime(df['datetimeUTC'])
    df['datetimeUTC'] = [dtUTC.tz_localize(tz='UTC') for dtUTC in
                         df['datetimeUTC']]
    df['datetime'] = [dtUTC.tz_convert(tz='America/New_York') for dtUTC in
                      df['datetimeUTC']]
    df['dayofweek'] = df['datetime'].dt.dayofweek
    df['hour'] = df['datetime'].dt.hour

    # calculate z-scores
    df = pd.merge(df, df_exp, how='left',
                  on=['dayofweek', 'hour', 'zone_id'])
    del df_exp
    df_std = df[['datetimeUTC', 'zone_id']]
    df_std['z_integrated_load'] = \
        (df['integrated_load'] - df['mean_integrated_load']) \
        / df['var_integrated_load']
    df_std = df_std.set_index(['datetimeUTC', 'zone_id'])
    del df

    # create table
    sql = """
                CREATE TABLE IF NOT EXISTS {table} (
                    rowid INTEGER PRIMARY KEY,
                    datetimeUTC TEXT,
                    zone_id INTEGER,
                    z_integrated_load FLOAT
                ); """.format(table=table)
    create_table(db_path=db_path, table=table, create_sql=sql, indexes=[],
                 overwrite=overwrite, verbose=verbose)

    # write data to table
    df_write = df_std.reset_index()
    df_write['datetimeUTC'] = df_write['datetimeUTC'].dt.tz_localize(
        None)
    df_to_table(db_path, df_write, table=table, overwrite=False,
                verbose=verbose)

    if verbose >= 1:
        output('Finished creating or updating {table} table. Dataframe shape '
               'is '.format(table=table) + str(df_std.shape) + '.')

    return df_std


def import_load(dl_dir, db_path, to_zoneid=False, zones_path=None,
                overwrite=False, verbose=0):
    """Loads, cleans, and imports nyiso load data into a sqlite database.
    Currently only imports palIntegrated files (i.e. integrated real-time
    load data).

    Parameters
    ----------
    dl_dir : str
        Path to the directory containing downloaded zip files. Imports
        all files in directory. Assumes each zip file is of the following
        format: 'yearmonth01palIntegrated_csv.zip' (e.g.
        '20121001palIntegrated_csv.zip').

    db_path : str
        Path to sqlite database.

    to_zoneid : bool
        If True, converts zone names to zone ids, based on zones_path csv
        (zones_path must be defined if True). If False, leaves zones_name
        column.

    zones_path : str or None
        Path to csv mapping zone_id to zone_name. Required if to_zoneid is True.

    overwrite : bool
        Defines whether or not to overwrite existing database tables.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    import_num : int
        Number of files imported into database.

    Notes
    -----
    """

    if to_zoneid:
        zone_str = 'zone_id'
        zone_field = 'zone_id INTEGER'
    else:
        zone_str = 'zone_name'
        zone_field = 'zone_name TEXT'

    # get files
    pattern = re.compile('\d{8}palIntegrated_csv.zip')
    files = get_regex_files(dl_dir, pattern=pattern, verbose=verbose)

    # create load table (if needed)
    create_sql = """
                CREATE TABLE IF NOT EXISTS load (
                    rowid INTEGER PRIMARY KEY,
                    datetimeUTC TEXT,
                    {zone_field} TEXT,
                    integrated_load REAL
                ); """.format(zone_field=zone_field)
    indexes = ['CREATE INDEX IF NOT EXISTS datetimeUTC_{zone_str} '
               'ON load (datetimeUTC, {zone_str});'.format(zone_str=zone_str)]
    create_table(db_path, 'load', create_sql, indexes=indexes,
                 overwrite=overwrite,
                 verbose=verbose)

    # load, clean, and import load data into table
    import_num = 0
    for file in files:
        if verbose >= 1:
            output('Started importing \"' + file + '\".')
        date = pd.Timestamp(file[0:8]).date()
        last_day = calendar.monthrange(date.year, date.month)[1]
        start_date = pd.Timestamp(year=date.year, month=date.month, day=1)
        end_date = pd.Timestamp(year=date.year, month=date.month, day=last_day)
        dates = pd.date_range(start_date, end_date)
        for date in dates:
            date_str = date.strftime('%Y%m%d')

            # load and clean data for current date
            df = load_loaddate(date_str, load_type='palIntegrated',
                               dl_dir=dl_dir, verbose=verbose)
            df = clean_palint(df, to_zoneid=to_zoneid, zones_path=zones_path,
                              verbose=verbose)

            # write to database
            df_write = df.reset_index()
            df_write['datetimeUTC'] = df_write['datetimeUTC'].dt.tz_localize(
                None)
            df_to_table(db_path, df_write, table='load', overwrite=False)
            del df_write

            import_num += 1
        if verbose >= 1:
            output('Finished importing \"' + file + '\".')
    output('Finished importing ' + str(import_num) +
           ' files from \"{dl_dir}\".'.format(dl_dir=dl_dir))

    return import_num


def import_load_forecast(dl_dir, db_path, zones_path=None,
                         overwrite=False, verbose=0):
    """Loads, cleans, and imports nyiso load forecast data into a sqlite
    database.

    load_forecast_px column represents the forecast for the current
    row (i.e. datetime and zone) x days prior. E.g. the _p2 column for a row
    with datetime of 10/5/2012 01:00:00 contains the forecast for 10/5/2012
    01:00:00 from two days before (i.e. 10/3/2012).

    Parameters
    ----------
    dl_dir : str
        Path to the directory containing downloaded zip files. Imports
        all files in directory. Assumes each zip file is of the following
        format: 'yearmonth01isolf_csv.zip' (e.g. '20121001isolf_csv.zip').

    db_path : str
        Path to sqlite database.

    zones_path : str or None
        Path to csv mapping zone_id to zone_name. Required if to_zoneid is True.

    overwrite : bool
        Defines whether or not to overwrite existing database tables.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    import_num : int
        Number of files imported into database.

    Notes
    -----
    """

    # get files
    pattern = re.compile('\d{8}isolf_csv.zip')
    files = get_regex_files(dl_dir, pattern=pattern, verbose=verbose)

    # create load table (if needed)
    create_sql = """
                    CREATE TABLE IF NOT EXISTS load_forecast (
                        rowid INTEGER PRIMARY KEY,
                        datetimeNY TEXT,
                        datetimeUTC TEXT,
                        zone_id INTEGER,
                        load_forecast_p0 REAL,
                        load_forecast_p1 REAL,
                        load_forecast_p2 REAL,
                        load_forecast_p3 REAL,
                        load_forecast_p4 REAL,
                        load_forecast_p5 REAL,
                        load_forecast_p6 REAL
                    ); """
    indexes = ['CREATE INDEX IF NOT EXISTS load_forecast_datetimeNY_zone_id '
               'ON load_forecast (datetimeNY, zone_id);',
               'CREATE UNIQUE INDEX IF NOT EXISTS '
               'load_forecast_datetimeUTC_zone_id ON load_forecast '
               '(datetimeUTC, zone_id);'
               ]
    create_table(db_path, 'load_forecast', create_sql, indexes=indexes,
                 overwrite=overwrite,
                 verbose=verbose)

    # load, clean, and import load data into table
    import_num = 0
    for file in files:
        if verbose >= 1:
            output('Started importing \"' + file + '\".')
        date = pd.Timestamp(file[0:8]).date()
        last_day = calendar.monthrange(date.year, date.month)[1]
        start_date = pd.Timestamp(year=date.year, month=date.month, day=1)
        end_date = pd.Timestamp(year=date.year, month=date.month, day=last_day)
        dates = pd.date_range(start_date, end_date)
        for date in dates:
            date_str = date.strftime('%Y%m%d')

            # load and clean data for current date
            df = load_loaddate(date_str, load_type='isolf',
                               dl_dir=dl_dir, verbose=verbose)
            df = clean_isolf(df, to_zoneid=True, zones_path=zones_path,
                             verbose=verbose)

            # write to database
            conn = connect_db(db_path)
            c = conn.cursor()
            df_write = df.reset_index()
            df_write['datetimeNY'] = df_write['datetimeNY'].dt.tz_localize(None)
            df_write['datetimeUTC'] = df_write['datetimeUTC'].dt.tz_localize(
                None)
            for index, row in df_write.iterrows():
                dtNY = row['datetimeNY']
                dtUTC = row['datetimeUTC']
                zone = row['zone_id']
                val = row.drop(
                    ['datetimeNY', 'zone_id', 'datetimeUTC']).dropna()
                col_name = val.index.values[0]
                sql = """
                    INSERT INTO load_forecast (datetimeNY, datetimeUTC, zone_id, 
                        {col_name})
                    VALUES ("{dtNY}", "{dtUTC}", {zone}, {val})
                    ON CONFLICT(datetimeUTC, zone_id) DO
                    UPDATE SET {col_name} = excluded.{col_name}
                ;""".format(col_name=col_name, val=val[0], dtNY=dtNY,
                            dtUTC=dtUTC, zone=zone)
                c.execute(sql)
                conn.commit()
            conn.close()

            import_num += 1
        if verbose >= 1:
            output('Finished importing \"' + file + '\".')
    output('Finished importing ' + str(import_num) +
           ' files from \"{dl_dir}\".'.format(dl_dir=dl_dir))

    return import_num


def load_loaddate(date, load_type, dl_dir, verbose=0):
    """Loads a nyiso load data file (one day of data) into a dataframe.
    Assumes the file is zipped with other files for that month.

    Parameters
    ----------
    date : str
        Date to load data for. Assumes 'yearmonthday' format (e.g. '20121030').

    load_type : str
        Defines type of load data. Current valid arguments: 'palIntegrated' (
        integrated real-time) and 'isolf' (load forecast).

    dl_dir : str
        Path to the directory containing downloaded zip files. Assumes each
        zip file is of the following format:
        'yearmonth01{load_type}_csv.zip' (e.g. '20121001palIntegrated_csv.zip').

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe
        Dataframe of one day of load data.

    Notes
    -----
    """

    if verbose >= 2:
        output('Started loading {load_type} file for {date} from '
               '\"{dl_dir}\".'.format(load_type=load_type, date=date,
                                  dl_dir=dl_dir))

    if load_type not in ['palIntegrated', 'isolf']:
        raise ValueError('Unknown type argument: {load_type}. See docs for '
                         'valid types'.format(load_type=load_type))
    elif len(date) != 8:
        raise ValueError('Incorrect format for date argument: {date}. Must '
                         'be yearmonthday with 8 characters.'.format(date=date))

    # read file into dataframe
    zip_path = dl_dir + date[0:6] + '01{load_type}_csv.zip'.format(
        load_type=load_type)
    file_path = date + load_type + '.csv'
    with zipfile.ZipFile(zip_path) as zip_file:
        with zip_file.open(file_path) as csv_file:
            df = pd.read_csv(csv_file)

    if verbose >= 2:
        output('Finished loading {load_type} file for {date} from '
               '\"{dl_dir}\".'.format(load_type=load_type, date=date,
                                  dl_dir=dl_dir))

    return df
