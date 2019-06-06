# -*- coding: utf-8 -*-
"""
Functions for analyzing NYC TLC datasets.


"""

import numpy as np
import pandas as pd
from shapely import geometry as geo
from twitterinfrastructure.tools import connect_db, create_table, \
    df_to_table, output, query, read_shapefile


def add_date_hour(df, verbose=0):
    """Adds date and hour columns to a dataframe. Assumes the dataframe has
    already been cleaned.

    Parameters
    ----------
    df : dataframe
        Dataframe to add date and hour columns to.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe
        Dataframe with added columns.

    Notes
    -----
    """

    if verbose >= 1:
        output('Started to add date and hour columns.')

    col_names = list(pd.Series(df.columns.values))
    if ('pickup_datetime' in col_names) and ('dropoff_datetime' in col_names):
        df['pickup_date'] = df['pickup_datetime'].dt.date
        df['pickup_hour'] = df['pickup_datetime'].dt.hour
        df['dropoff_date'] = df['dropoff_datetime'].dt.date
        df['dropoff_hour'] = df['dropoff_datetime'].dt.hour
        df.sort_values(['pickup_date', 'pickup_hour'], inplace=True)

        if verbose >= 1:
            output('Finished adding (and sorting by) date and hour columns.')

    elif verbose >= 1:
        output('Unable to add date and hour columns due to missing datetime '
               'columns.')

    return df


def add_location_id(df, shapefile_path, overwrite=False, verbose=0):
    """Adds location_id values to a dataframe for records with lat/lon
    values. Assumes the dataframe has already been cleaned.

    Parameters
    ----------
    df : dataframe
        Dataframe to add location_id values for.

    shapefile_path : str
        Path to shapefile containing location boundaries.

    overwrite : bool
        If True, overwrites any existing location_id values. If False,
        keeps any existing location_id values (this assumes the shapefile
        contains the same zones and location_id values as those used for any
        existing location_id values in the dataframe).

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe
        Dataframe with added values.

    Notes
    -----
    """

    if verbose >= 1:
        output('Started adding/updating location_id column.')

    col_names = list(pd.Series(df.columns.values))
    if ('pickup_longitude' in col_names) and \
            ('pickup_latitude' in col_names) and \
            ('dropoff_longitude' in col_names) and \
            ('dropoff_latitude' in col_names):

        # get lat/lon values
        pickup_lats = df['pickup_latitude']
        pickup_lons = df['pickup_longitude']
        dropoff_lats = df['dropoff_latitude']
        dropoff_lons = df['dropoff_longitude']

        # get location_id values
        pickup_location_ids, _ = points_in_shapefile(pickup_lats,
                                                     pickup_lons,
                                                     shapefile_path)
        dropoff_location_ids, _ = points_in_shapefile(dropoff_lats,
                                                      dropoff_lons,
                                                      shapefile_path)

        # replace new location_id values with current/existing ones (if
        # overwrite set to True and column currently exists)
        if not overwrite and ('pickup_location_id' in col_names):
            pickup_location_ids = [id_curr if pd.notnull(id_curr) else id_new
                                   for id_curr, id_new in
                                   zip(df['pickup_location_id'],
                                       pickup_location_ids)]
        if not overwrite and ('dropoff_location_id' in col_names):
            dropoff_location_ids = [id_curr if pd.notnull(id_curr) else id_new
                                    for id_curr, id_new in
                                    zip(df['dropoff_location_id'],
                                        dropoff_location_ids)]

        # add or update location_id columns
        df['pickup_location_id'] = pickup_location_ids
        df['dropoff_location_id'] = dropoff_location_ids

        if verbose >= 1:
            output('Finished adding/updating location_id column for shapefile '
                   + shapefile_path + '.')

    elif verbose >= 1:
        output('Unable to add/update location_id column due to missing lat/lon '
               'columns.')

    return df


def create_expected_zone_date(db_path, summary_table, taxi_zones_table,
                              datetime_range_ref, datetime_range_excl=None,
                              pickup=False, title=None, overwrite=False,
                              verbose=0):
    """Creates a table and dataframe of expected data from the summary_table
    table. Expectation includes mean and variance of mean_pace and trip_count
    for the specified reference datetime range. Expectation is calculated for
    every possible dayofweek-zone combination, with NaNs for those missing data.

    Parameters
    ----------
    db_path : str
        Path to sqlite database to create or connect to.

    summary_table : str
        Name of the db summary table containing data to calculate
        standardized mean_pace from.

    taxi_zones_table : str
        Name of the taxi_zones db table specifying possible zone location_id
        values.

    datetime_range_ref : tuple
        Specifies the start and end of the reference time period to use when
        calculating expected values (inclusive). Specify as a 2-element
        tuple of datetime strings with year-month-day and hour:minutes:seconds.

    datetime_range_excl : tuple
        Specifies the start and end of time period to exclude from reference
        time period. Specify as a 2-element
        tuple of datetime strings with year-month-day and hour:minutes:seconds.

    pickup : bool
        If True, based on pickup zone. If False, based on dropoff zone.

    title : str
        Defines the suffix of the expected_zone[pickup/dropoff]_day_[title]
        table to be created.

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
    """

    # define column and table names
    if pickup:
        datetime_col = 'pickup_datetime'
        locationid_col = 'pickup_location_id'
        table = 'expected_zonepickup_date_{title}'.format(title=title)
        if 'dropoff' in summary_table:
            output('Warning: dropoff found in summary_table argument but '
                   'pickup argument set to True', 'create_expected_zone_date')
    else:
        datetime_col = 'dropoff_datetime'
        locationid_col = 'dropoff_location_id'
        table = 'expected_zonedropoff_date_{title}'.format(title=title)
        if 'pickup' in summary_table:
            output('Warning: pickup found in summary_table argument but '
                   'pickup argument set to False', 'create_expected_zone_date')

    if verbose >= 1:
        output('Started creating or updating {table} table.'.format(
            table=table))

    # query range of location_id values to consider
    sql = """
            SELECT MIN(location_id) AS min_location_id,
                MAX(location_id) AS max_location_id
            FROM {taxi_zones_table}
          """.format(taxi_zones_table=taxi_zones_table)
    df_zones = query(db_path, sql)
    location_ids = range(df_zones['min_location_id'].values[0],
                         df_zones['max_location_id'].values[0] - 1)
    del df_zones

    # query reference data
    if datetime_range_excl:
        sql = """
            SELECT {datetime_col}, {locationid_col}, mean_pace, trip_count
            FROM {summary_table}
            WHERE
                ({datetime_col} BETWEEN "{start_datetime}" AND "{end_datetime}")
                AND ({datetime_col} NOT BETWEEN "{start_datetime_excl}" AND
                    "{end_datetime_excl}")
        ;""".format(datetime_col=datetime_col,
                    locationid_col=locationid_col,
                    summary_table=summary_table,
                    start_datetime=datetime_range_ref[0],
                    end_datetime=datetime_range_ref[1],
                    start_datetime_excl=datetime_range_excl[0],
                    end_datetime_excl=datetime_range_excl[1])
    else:
        sql = """
            SELECT {datetime_col}, {locationid_col}, mean_pace, trip_count
            FROM {summary_table}
            WHERE
                ({datetime_col} BETWEEN "{start_datetime}" AND "{end_datetime}")
        ;""".format(datetime_col=datetime_col,
                    locationid_col=locationid_col,
                    summary_table=summary_table,
                    start_datetime=datetime_range_ref[0],
                    end_datetime=datetime_range_ref[1])
    df = query(db_path, sql)
    df['mean_pace'] = df['mean_pace'] / 60  # convert mean_pace to [min./mile]

    # add dayofweek (0 = Monday)
    df[datetime_col] = pd.to_datetime(df[datetime_col])
    df['dayofweek'] = df[datetime_col].dt.dayofweek

    # calculate mean and variance for each dayofweek-zone combination
    expected = []
    for dayofweek in range(7):
        for location_id in location_ids:
            # filter to current dayofweek and zone
            df_filter = df[(df['dayofweek'] == dayofweek) &
                           (df[locationid_col] == location_id)]

            # calculate mean and variance
            if not df_filter.empty:
                mean_meanpace = np.mean(df_filter['mean_pace'].values)
                var_meanpace = np.var(df_filter['mean_pace'].values)
                mean_tripcount = np.mean(df_filter['trip_count'].values)
                var_tripcount = np.var(df_filter['trip_count'].values)
                min_tripcount = min(df_filter['trip_count'].values)
                num_rows = df_filter.shape[0]
                expected.append([dayofweek, location_id,
                                 mean_meanpace, var_meanpace,
                                 mean_tripcount, var_tripcount,
                                 min_tripcount, num_rows])
            else:
                expected.append([dayofweek, location_id,
                                 np.nan, np.nan, np.nan, np.nan,
                                 np.nan, np.nan])
    df_exp = pd.DataFrame(expected,
                          columns=['dayofweek', locationid_col,
                                   'mean_mean_pace', 'var_mean_pace',
                                   'mean_trip_count', 'var_trip_count',
                                   'min_trip_count', 'num_rows'])
    df_exp.set_index(['dayofweek', locationid_col])

    # create table
    sql = """
            CREATE TABLE IF NOT EXISTS {table} (
                rowid INTEGER PRIMARY KEY,
                dayofweek INTEGER,
                {locationid_col} INTEGER,
                mean_mean_pace FLOAT,
                var_mean_pace FLOAT,
                mean_trip_count FLOAT,
                var_trip_count FLOAT,
                min_trip_count FLOAT,
                num_rows INTEGER
            ); """.format(table=table, locationid_col=locationid_col)
    create_table(db_path=db_path, table=table, create_sql=sql, indexes=[],
                 overwrite=overwrite, verbose=verbose)

    # write data to table
    df_to_table(db_path, df_exp, table=table, overwrite=False,
                verbose=verbose)

    if verbose >= 1:
        output('Finished creating or updating {table} table. Dataframe shape '
               'is '.format(table=table) + str(df_exp.shape) + '.')

    return df_exp


def create_expected_zone_hour(db_path, summary_table, taxi_zones_table,
                              datetime_range_ref, datetime_range_excl=None,
                              pickup=False, title=None, overwrite=False,
                              verbose=0):
    """Creates a table and dataframe of expected data from the summary_table
    table. Expectation includes mean and variance of mean_pace and trip_count
    for the specified reference datetime range. Expectation is calculated for
    every possible dayofweek-hour-zone combination, with NaNs for those
    missing data.

    Parameters
    ----------
    db_path : str
        Path to sqlite database to create or connect to.

    summary_table : str
        Name of the db summary table containing data to calculate
        standardized mean_pace from.

    taxi_zones_table : str
        Name of the taxi_zones db table specifying possible zone location_id
        values.

    datetime_range_ref : tuple
        Specifies the start and end of the reference time period to use when
        calculating expected values (inclusive). Specify as a 2-element
        tuple of datetime strings with year-month-day and hour:minutes:seconds.

    datetime_range_excl : tuple
        Specifies the start and end of time period to exclude from reference
        time period. Specify as a 2-element
        tuple of datetime strings with year-month-day and hour:minutes:seconds.

    pickup : bool
        If True, based on pickup zone. If False, based on dropoff zone.

    title : str
        Defines the suffix of the expected_zone[pickup/dropoff]_day_[title]
        table to be created.

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
    """

    # define column and table names
    if pickup:
        datetime_col = 'pickup_datetime'
        locationid_col = 'pickup_location_id'
        table = 'expected_zonepickup_hour_{title}'.format(title=title)
        if 'dropoff' in summary_table:
            output('Warning: dropoff found in summary_table argument but '
                   'pickup argument set to True', 'create_expected_zone_hour')
    else:
        datetime_col = 'dropoff_datetime'
        locationid_col = 'dropoff_location_id'
        table = 'expected_zonedropoff_hour_{title}'.format(title=title)
        if 'pickup' in summary_table:
            output('Warning: pickup found in summary_table argument but '
                   'pickup argument set to False', 'create_expected_zone_hour')

    if verbose >= 1:
        output('Started creating or updating {table} table.'.format(
            table=table))

    # query range of location_id values to consider
    sql = """
            SELECT MIN(location_id) AS min_location_id,
                MAX(location_id) AS max_location_id
            FROM {taxi_zones_table}
          """.format(taxi_zones_table=taxi_zones_table)
    df_zones = query(db_path, sql)
    location_ids = range(df_zones['min_location_id'].values[0],
                         df_zones['max_location_id'].values[0] - 1)
    del df_zones

    # query reference data
    if datetime_range_excl:
        sql = """
            SELECT {datetime_col}, {locationid_col}, mean_pace, trip_count
            FROM {summary_table}
            WHERE
                ({datetime_col} BETWEEN "{start_datetime}" AND "{end_datetime}")
                AND ({datetime_col} NOT BETWEEN "{start_datetime_excl}" AND
                    "{end_datetime_excl}")
        ;""".format(datetime_col=datetime_col,
                    locationid_col=locationid_col,
                    summary_table=summary_table,
                    start_datetime=datetime_range_ref[0],
                    end_datetime=datetime_range_ref[1],
                    start_datetime_excl=datetime_range_excl[0],
                    end_datetime_excl=datetime_range_excl[1])
    else:
        sql = """
            SELECT {datetime_col}, {locationid_col}, mean_pace, trip_count
            FROM {summary_table}
            WHERE
                ({datetime_col} BETWEEN "{start_datetime}" AND "{end_datetime}")
        ;""".format(datetime_col=datetime_col,
                    locationid_col=locationid_col,
                    summary_table=summary_table,
                    start_datetime=datetime_range_ref[0],
                    end_datetime=datetime_range_ref[1])
    df = query(db_path, sql)
    df['mean_pace'] = df['mean_pace'] / 60  # convert mean_pace to [min./mile]

    # add dayofweek (0 = Monday) and hour (0-23)
    df[datetime_col] = pd.to_datetime(df[datetime_col])
    df['dayofweek'] = df[datetime_col].dt.dayofweek
    df['hour'] = df[datetime_col].dt.hour

    # calculate mean and variance for each dayofweek-hour-zone combination
    expected = []
    for dayofweek in range(7):
        for hour in range(24):
            for location_id in location_ids:
                # filter to current dayofweek, hour, and zone
                df_filter = df[(df['dayofweek'] == dayofweek) &
                               (df['hour'] == hour) &
                               (df[locationid_col] == location_id)]

                # calculate mean and variance
                if not df_filter.empty:
                    mean_meanpace = np.mean(df_filter['mean_pace'].values)
                    var_meanpace = np.var(df_filter['mean_pace'].values)
                    mean_tripcount = np.mean(df_filter['trip_count'].values)
                    var_tripcount = np.var(df_filter['trip_count'].values)
                    min_tripcount = min(df_filter['trip_count'].values)
                    num_rows = df_filter.shape[0]
                    expected.append([dayofweek, hour, location_id,
                                     mean_meanpace, var_meanpace,
                                     mean_tripcount, var_tripcount,
                                     min_tripcount, num_rows])
                else:
                    expected.append([dayofweek, hour, location_id,
                                     np.nan, np.nan, np.nan, np.nan,
                                     np.nan, np.nan])
    df_exp = pd.DataFrame(expected,
                          columns=['dayofweek', 'hour', locationid_col,
                                   'mean_mean_pace', 'var_mean_pace',
                                   'mean_trip_count', 'var_trip_count',
                                   'min_trip_count', 'num_rows'])
    df_exp.set_index(['dayofweek', 'hour', locationid_col])

    # create table
    sql = """
            CREATE TABLE IF NOT EXISTS {table} (
                rowid INTEGER PRIMARY KEY,
                dayofweek INTEGER,
                hour INTEGER,
                {locationid_col} INTEGER,
                mean_mean_pace FLOAT,
                var_mean_pace FLOAT,
                mean_trip_count FLOAT,
                var_trip_count FLOAT,
                min_trip_count FLOAT,
                num_rows INTEGER
            ); """.format(table=table, locationid_col=locationid_col)
    create_table(db_path=db_path, table=table, create_sql=sql, indexes=[],
                 overwrite=overwrite, verbose=verbose)

    # write data to table
    df_to_table(db_path, df_exp, table=table, overwrite=False,
                verbose=verbose)

    if verbose >= 1:
        output('Finished creating or updating {table} table. Dataframe shape '
               'is '.format(table=table) + str(df_exp.shape) + '.')

    return df_exp


def create_standard_zone_date(db_path, summary_table, expected_table,
                              datetime_range, pickup=False, min_num_rows=5,
                              title=None, overwrite=False, verbose=0):
    """Creates a table and dataframe of standardized data from the
    summary_table table (for data grouped by dayofweek-zone. Standardization
    is relative to the mean and variance of corresponding data from the
    specified reference datetime range (saved as an expected_[] table in the
    database).

    Parameters
    ----------
    db_path : str
        Path to sqlite database to create or connect to.

    summary_table : str
        Name of the db table containing summary data to calculate
        standardized mean_pace for.

    expected_table : str
        Name of the db table containing expected data (i.e. mean and
        variance) to calculate standardized mean_pace from.

    datetime_range : tuple
        Specifies the start and end of the time period to calculate
        standardized mean_pace for (inclusive). Specify as a 2-element
        tuple of datetime strings with year-month-day and hour:minutes:seconds.
        E.g. ('2012-10-29 00:00:00', '2012-11-03 23:59:59') to calculate
        standardized mean_pace for times between 10/29/2012 and 11/03/2012.

    pickup : bool
        If True, based on pickup zone. If False, based on dropoff zone.

    min_num_rows : int
        Defines the minimum number of rows needed in the reference set to
        standardize data.

    title : str
        Defines the suffix of the standard_zone[pickup/dropoff]_[time]_[title]
        table to be created.

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

    # define column and table names
    if pickup:
        datetime_col = 'pickup_datetime'
        locationid_col = 'pickup_location_id'
        table = 'standard_zonepickup_date_{title}'.format(title=title)
        if 'dropoff' in summary_table:
            output('Warning: dropoff found in summary_table argument but '
                   'pickup argument set to True', 'create_standard_zone_date')
    else:
        datetime_col = 'dropoff_datetime'
        locationid_col = 'dropoff_location_id'
        table = 'standard_zonedropoff_date_{title}'.format(title=title)
        if 'pickup' in summary_table:
            output('Warning: pickup found in summary_table argument but '
                   'pickup argument set to False', 'create_standard_zone_date')

    if verbose >= 1:
        output('Started creating or updating {table} table.'.format(
            table=table))

    # query expected values calculated from at least min_num_rows data points
    sql = """
            SELECT * FROM {expected_table} 
            WHERE num_rows >= {min_num_rows};""".format(
        expected_table=expected_table, min_num_rows=min_num_rows)
    df_exp = query(db_path, sql)
    df_exp = df_exp[['dayofweek', locationid_col, 'mean_mean_pace',
                     'var_mean_pace', 'mean_trip_count', 'var_trip_count']]

    # query data to standardize
    sql = """
            SELECT {datetime_col}, {locationid_col}, mean_pace, trip_count
            FROM {summary_table}
            WHERE
                {datetime_col} BETWEEN "{start_datetime}" AND "{end_datetime}";
            """.format(datetime_col=datetime_col,
                       locationid_col=locationid_col,
                       summary_table=summary_table,
                       start_datetime=datetime_range[0],
                       end_datetime=datetime_range[1])
    df = query(db_path, sql)
    df['mean_pace'] = df['mean_pace'] / 60  # convert mean_pace to [min./mile]

    # add dayofweek (0 = Monday) and hour (0-23)
    df[datetime_col] = pd.to_datetime(df[datetime_col])
    df['dayofweek'] = df[datetime_col].dt.dayofweek

    # calculate mean and variance for each dayofweek-hour-zone combination
    df = pd.merge(df, df_exp, how='left',
                  on=['dayofweek', locationid_col])
    del df_exp
    df_std = df[[datetime_col, locationid_col, 'trip_count']]
    df_std['z_mean_pace'] = (df['mean_pace'] - df['mean_mean_pace']) \
        / df['var_mean_pace']
    df_std['z_trip_count'] = (df['trip_count'] - df['mean_trip_count']) \
        / df['var_trip_count']
    del df

    # create table
    sql = """
                CREATE TABLE IF NOT EXISTS {table} (
                    rowid INTEGER PRIMARY KEY,
                    {datetime_col} TEXT,
                    {locationid_col} INTEGER,
                    z_mean_pace FLOAT,
                    z_trip_count FLOAT,
                    trip_count INTEGER
                ); """.format(table=table, datetime_col=datetime_col,
                              locationid_col=locationid_col)
    create_table(db_path=db_path, table=table, create_sql=sql, indexes=[],
                 overwrite=overwrite, verbose=verbose)

    # write data to table
    df_to_table(db_path, df_std, table=table, overwrite=False, verbose=verbose)

    if verbose >= 1:
        output('Finished creating or updating {table} table. Dataframe shape '
               'is '.format(table=table) + str(df_std.shape) + '.')

    return df_std


def create_standard_zone_hour(db_path, summary_table, expected_table,
                              datetime_range, pickup=False, min_num_rows=5,
                              title=None, overwrite=False, verbose=0):
    """Creates a table and dataframe of standardized data from the
    summary_table table (for data grouped by dayofweek-hour-zone.
    Standardization is relative to the mean and variance of corresponding
    data from the specified reference datetime range (saved as an expected_[]
    table in the database).

    Parameters
    ----------
    db_path : str
        Path to sqlite database to create or connect to.

    summary_table : str
        Name of the db table containing summary data to calculate
        standardized mean_pace for.

    expected_table : str
        Name of the db table containing expected data (i.e. mean and
        variance) to calculate standardized mean_pace from.

    datetime_range : tuple
        Specifies the start and end of the time period to calculate
        standardized mean_pace for (inclusive). Specify as a 2-element
        tuple of datetime strings with year-month-day and hour:minutes:seconds.
        E.g. ('2012-10-29 00:00:00', '2012-11-03 23:59:59') to calculate
        standardized mean_pace for times between 10/29/2012 and 11/03/2012.

    pickup : bool
        If True, based on pickup zone. If False, based on dropoff zone.

    min_num_rows : int
        Defines the minimum number of rows needed in the reference set to
        standardize data.

    title : str
        Defines the suffix of the standard_zone[pickup/dropoff]_[time]_[title]
        table to be created.

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

    # define column and table names
    if pickup:
        datetime_col = 'pickup_datetime'
        locationid_col = 'pickup_location_id'
        table = 'standard_zonepickup_hour_{title}'.format(title=title)
        if 'dropoff' in summary_table:
            output('Warning: dropoff found in summary_table argument but '
                   'pickup argument set to True', 'create_standard_zone_hour')
    else:
        datetime_col = 'dropoff_datetime'
        locationid_col = 'dropoff_location_id'
        table = 'standard_zonedropoff_hour_{title}'.format(title=title)
        if 'pickup' in summary_table:
            output('Warning: pickup found in summary_table argument but '
                   'pickup argument set to False', 'create_standard_zone_hour')

    if verbose >= 1:
        output('Started creating or updating {table} table.'.format(
            table=table))

    # query expected values calculated from at least min_num_rows data points
    sql = """
            SELECT * FROM {expected_table} 
            WHERE num_rows >= {min_num_rows};""".format(
        expected_table=expected_table, min_num_rows=min_num_rows)
    df_exp = query(db_path, sql)
    df_exp = df_exp[['dayofweek', 'hour', locationid_col, 'mean_mean_pace',
                     'var_mean_pace', 'mean_trip_count', 'var_trip_count']]

    # query data to standardize
    sql = """
            SELECT {datetime_col}, {locationid_col}, mean_pace, trip_count
            FROM {summary_table}
            WHERE
                {datetime_col} BETWEEN "{start_datetime}" AND "{end_datetime}";
            """.format(datetime_col=datetime_col,
                       locationid_col=locationid_col,
                       summary_table=summary_table,
                       start_datetime=datetime_range[0],
                       end_datetime=datetime_range[1])
    df = query(db_path, sql)
    df['mean_pace'] = df['mean_pace'] / 60  # convert mean_pace to [min./mile]

    # add dayofweek (0 = Monday) and hour (0-23)
    df[datetime_col] = pd.to_datetime(df[datetime_col])
    df['dayofweek'] = df[datetime_col].dt.dayofweek
    df['hour'] = df[datetime_col].dt.hour

    # calculate mean and variance for each dayofweek-hour-zone combination
    df = pd.merge(df, df_exp, how='left',
                  on=['dayofweek', 'hour', locationid_col])
    del df_exp
    df_std = df[[datetime_col, locationid_col, 'trip_count']]
    df_std['z_mean_pace'] = (df['mean_pace'] - df['mean_mean_pace']) \
        / df['var_mean_pace']
    df_std['z_trip_count'] = (df['trip_count'] - df['mean_trip_count']) \
        / df['var_trip_count']
    del df

    # create table
    sql = """
                CREATE TABLE IF NOT EXISTS {table} (
                    rowid INTEGER PRIMARY KEY,
                    {datetime_col} TEXT,
                    {locationid_col} INTEGER,
                    z_mean_pace FLOAT,
                    z_trip_count FLOAT,
                    trip_count INTEGER
                ); """.format(table=table, datetime_col=datetime_col,
                              locationid_col=locationid_col)
    create_table(db_path=db_path, table=table, create_sql=sql, indexes=[],
                 overwrite=overwrite, verbose=verbose)

    # write data to table
    df_to_table(db_path, df_std, table=table, overwrite=False, verbose=verbose)

    if verbose >= 1:
        output('Finished creating or updating {table} table. Dataframe shape '
               'is '.format(table=table) + str(df_std.shape) + '.')

    return df_std


def create_summary_route_time(db_path, byborough=True, byday=True,
                              title=None,
                              trips_analysis_table='trips_analysis_',
                              taxi_zones_table=None,
                              overwrite=False, verbose=0):
    """Creates a table and dataframe of summary statistics for taxi traffic
    from the trips_analysis_table table grouped by borough/zone routes and
    day/hour time periods. Routes are defined by directional borough-borough
    pairs or zone-zone pairs.

    Parameters
    ----------
    db_path : str
        Path to sqlite database to create or connect to.

    byborough : bool
        If True, groups by borough. If False, groups by zone (i.e. location_id).

    byday : bool
        If True, groups by year-month-day. If False, groups by
        year-month-day-hour.

    title : str
        Defines the suffix of the summary_routeborough/zone_day/hour_[title]
        table to be created.

    trips_analysis_table : str
        Name of the trips_analysis db table containing data to summarize.
        Assumes this table has been properly cleaned (using
        clean_nyctlc.clean_yellow) and filtered (using create_trips_analysis).

    taxi_zones_table : str or None
        Name of the taxi_zones db table mapping zone location_id values to
        borough_id values (created from create_trips_analysis or
        create_taxi_zones). Only needed if byborough=True.

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
    total_duration in [seconds]
    total_distance in [miles]
    mean_pace in [miles/second]
    """

    if byborough and not taxi_zones_table:
        output('Error: Must provide taxi_zones_table if grouping by borough.',
               'create_summary')

    # define table name
    if byborough and byday:
        table = 'summary_routeborough_day_{title}'.format(title=title)
    elif byborough and not byday:
        table = 'summary_routeborough_hour_{title}'.format(title=title)
    elif not byborough and byday:
        table = 'summary_routezone_day_{title}'.format(title=title)
    elif not byborough and not byday:
        table = 'summary_routezone_hour_{title}'.format(title=title)
    else:
        raise ValueError('Invalid byborough and/or byday arguments.')

    if verbose >= 1:
        output('Started creating or updating {table} table.'.format(
            table=table))

    # calculate summary data
    if byborough and byday:
        sql = """
                SELECT pickup_date,
                    pu.borough_id AS pickup_borough_id,
                    do.borough_id AS dropoff_borough_id,
                    COUNT(trip_id) as trip_count,
                    SUM(trip_duration) AS total_duration,
                    SUM(trip_distance) AS total_distance
                FROM {trips_analysis_table}
                INNER JOIN
                    {taxi_zones_table} pu ON (pu.location_id =
                    {trips_analysis_table}.pickup_location_id)
                INNER JOIN
                    {taxi_zones_table} do ON (do.location_id =
                    {trips_analysis_table}.dropoff_location_id)
                GROUP BY pickup_date, pickup_borough_id, dropoff_borough_id;
                """.format(trips_analysis_table=trips_analysis_table,
                           taxi_zones_table=taxi_zones_table)
    elif byborough and not byday:
        sql = """
                SELECT pickup_date, pickup_hour,
                    pu.borough_id AS pickup_borough_id,
                    do.borough_id AS dropoff_borough_id,
                    COUNT(trip_id) as trip_count,
                    SUM(trip_duration) AS total_duration,
                    SUM(trip_distance) AS total_distance
                FROM {trips_analysis_table}
                INNER JOIN
                    {taxi_zones_table} pu ON (pu.location_id =
                    {trips_analysis_table}.pickup_location_id)
                INNER JOIN
                    {taxi_zones_table} do ON (do.location_id =
                    {trips_analysis_table}.dropoff_location_id)
                GROUP BY
                    pickup_date, pickup_hour,
                    pickup_borough_id, dropoff_borough_id;
                """.format(trips_analysis_table=trips_analysis_table,
                           taxi_zones_table=taxi_zones_table)
    elif not byborough and byday:
        sql = """
                SELECT pickup_date, pickup_location_id, dropoff_location_id,
                    COUNT(trip_id) as trip_count,
                    SUM(trip_duration) AS total_duration,
                    SUM(trip_distance) AS total_distance
                FROM {trips_analysis_table}
                GROUP BY pickup_date, pickup_location_id, dropoff_location_id;
                """.format(trips_analysis_table=trips_analysis_table)
    elif not byborough and not byday:
        sql = """
                SELECT pickup_date, pickup_hour,
                    pickup_location_id, dropoff_location_id,
                    COUNT(trip_id) as trip_count,
                    SUM(trip_duration) AS total_duration,
                    SUM(trip_distance) AS total_distance
                FROM {trips_analysis_table}
                GROUP BY pickup_date, pickup_hour, pickup_location_id,
                    dropoff_location_id;
                """.format(trips_analysis_table=trips_analysis_table)
    df = query(db_path, sql)

    # add calculated mean_pace
    df['mean_pace'] = df['total_duration'] / df['total_distance']

    # create table (if not exists)
    if byborough and byday:
        sql = """
                CREATE TABLE IF NOT EXISTS {table} (
                    route_day_id INTEGER PRIMARY KEY,
                    pickup_date TEXT,
                    pickup_borough_id INTEGER,
                    dropoff_borough_id INTEGER,
                    trip_count INTEGER,
                    total_duration INTEGER,
                    total_distance FLOAT,
                    mean_pace FLOAT
                ); """.format(table=table)
    elif byborough and not byday:
        sql = """
                CREATE TABLE IF NOT EXISTS {table} (
                    route_hour_id INTEGER PRIMARY KEY,
                    pickup_date TEXT,
                    pickup_hour INTEGER,
                    pickup_borough_id INTEGER,
                    dropoff_borough_id INTEGER,
                    trip_count INTEGER,
                    total_duration INTEGER,
                    total_distance FLOAT,
                    mean_pace FLOAT
                ); """.format(table=table)
    elif not byborough and byday:
        sql = """
                CREATE TABLE IF NOT EXISTS {table} (
                    route_hour_id INTEGER PRIMARY KEY,
                    pickup_date TEXT,
                    pickup_location_id INTEGER,
                    dropoff_location_id INTEGER,
                    trip_count INTEGER,
                    total_duration INTEGER,
                    total_distance FLOAT,
                    mean_pace FLOAT
                ); """.format(table=table)
    elif not byborough and not byday:
        sql = """
                CREATE TABLE IF NOT EXISTS {table} (
                    route_hour_id INTEGER PRIMARY KEY,
                    pickup_date TEXT,
                    pickup_hour INTEGER,
                    pickup_location_id INTEGER,
                    dropoff_location_id INTEGER,
                    trip_count INTEGER,
                    total_duration INTEGER,
                    total_distance FLOAT,
                    mean_pace FLOAT
                ); """.format(table=table)
    create_table(db_path=db_path, table=table, create_sql=sql, indexes=[],
                 overwrite=overwrite, verbose=verbose)

    # write summary data to table
    df_to_table(db_path, df, table=table, overwrite=False, verbose=verbose)

    if verbose >= 1:
        output('Finished creating or updating {table} table. Dataframe shape '
               'is '.format(table=table) + str(df.shape) + '.')

    return df


def create_summary_zone(db_path, pickup=True, title=None,
                        trips_analysis_table='trips_analysis_',
                        overwrite=False, verbose=0):
    """Creates a table and dataframe of summary statistics for taxi traffic
    from the trips_analysis_table table grouped by pickup/dropoff zone (over
    all time).

    Parameters
    ----------
    db_path : str
        Path to sqlite database to create or connect to.

    pickup : bool
        If True, groups by pickup zone. If False, groups by dropoff zone.

    title : str
        Defines the suffix of the summary_zone[pickup/dropoff]_[title] table
        to be created.

    trips_analysis_table : str
        Name of the trips_analysis db table containing data to summarize.
        Assumes this table has been properly cleaned (using
        clean_nyctlc.clean_yellow) and filtered (using create_trips_analysis).

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
    total_duration in [seconds]
    total_distance in [miles]
    mean_pace in [miles/second]
    """

    # define column and table names
    if pickup:
        table = 'summary_zonepickup_' + title
        zonestr = 'pickup'
    else:
        table = 'summary_zonedropoff_' + title
        zonestr = 'dropoff'

    if verbose >= 1:
        output('Started creating or updating {table} table.'.format(
            table=table))

    # calculate grouped summary data
    sql = """
            SELECT {zonestr}_location_id,
                COUNT(trip_id) as trip_count,
                SUM(trip_duration) AS total_duration,
                SUM(trip_distance) AS total_distance
            FROM {trips_analysis_table}
            GROUP BY {zonestr}_location_id;
           """.format(zonestr=zonestr,
                      trips_analysis_table=trips_analysis_table)
    df = query(db_path, sql)

    # add calculated mean_pace
    df['mean_pace'] = df['total_duration'] / df['total_distance']

    # create table (if not exists)
    sql = """
            CREATE TABLE IF NOT EXISTS {table} (
                rowid INTEGER PRIMARY KEY,
                {zonestr}_location_id INTEGER,
                trip_count INTEGER,
                total_duration INTEGER,
                total_distance FLOAT,
                mean_pace FLOAT
            ); """.format(table=table, zonestr=zonestr)
    create_table(db_path=db_path, table=table, create_sql=sql, indexes=[],
                 overwrite=overwrite, verbose=verbose)

    # write summary data to table
    df_to_table(db_path, df, table=table, overwrite=False, verbose=verbose)

    if verbose >= 1:
        output('Finished creating or updating {table} table. Dataframe shape '
               'is '.format(table=table) + str(df.shape) + '.')

    return df


def create_summary_zone_time(db_path, pickup=True, bytime='hour', title=None,
                             trips_analysis_table='trips_analysis_',
                             overwrite=False, verbose=0):
    """Creates a table and dataframe of summary statistics for taxi traffic
    from the trips_analysis_table table grouped by pickup/dropoff zone and time.

    Parameters
    ----------
    db_path : str
        Path to sqlite database to create or connect to.

    pickup : bool
        If True, groups by pickup zone. If False, groups by dropoff zone.

    bytime : str
        Defines temporal grouping: 'hour' or 'date'.

    title : str
        Defines the suffix of the summary_zone[pickup/dropoff]_[bytime]_[title]
        table to be created.

    trips_analysis_table : str
        Name of the trips_analysis db table containing data to summarize.
        Assumes this table has been properly cleaned (using
        clean_nyctlc.clean_yellow) and filtered (using create_trips_analysis).

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
    total_duration in [seconds]
    total_distance in [miles]
    mean_pace in [miles/second]
    """

    # define column and table names
    if pickup:
        datetime_col = 'pickup_datetime'
        date_col = 'pickup_date'
        hour_col = 'pickup_hour'
        locationid_col = 'pickup_location_id'
        table = 'summary_zonepickup_{bytime}_{title}'.format(bytime=bytime,
                                                             title=title)
    else:
        datetime_col = 'dropoff_datetime'
        date_col = 'dropoff_date'
        hour_col = 'dropoff_hour'
        locationid_col = 'dropoff_location_id'
        table = 'summary_zonedropoff_{bytime}_{title}'.format(bytime=bytime,
                                                              title=title)

    if verbose >= 1:
        output('Started creating or updating {table} table.'.format(
            table=table))

    # check args
    if bytime not in ['hour', 'date']:
        raise ValueError('Invalid bytime argument.')

    # calculate grouped summary data
    if bytime == 'date':
        sql = """
                SELECT 
                    {date_col} AS {datetime_col},
                    {locationid_col},
                    COUNT(trip_id) as trip_count,
                    SUM(trip_duration) AS total_duration,
                    SUM(trip_distance) AS total_distance
                FROM {trips_analysis_table}
                GROUP BY {date_col}, {locationid_col};
            """.format(date_col=date_col, datetime_col=datetime_col,
                       locationid_col=locationid_col,
                       trips_analysis_table=trips_analysis_table)
    elif bytime == 'hour':
        sql = """
                SELECT 
                    {date_col} || " " || substr('00' || {hour_col}, 
                        -2, 2) || ":00:00" AS {datetime_col},
                    {locationid_col},
                    COUNT(trip_id) as trip_count,
                    SUM(trip_duration) AS total_duration,
                    SUM(trip_distance) AS total_distance
                FROM {trips_analysis_table}
                GROUP BY {date_col}, {hour_col}, {locationid_col};
            """.format(date_col=date_col, hour_col=hour_col,
                       datetime_col=datetime_col, locationid_col=locationid_col,
                       trips_analysis_table=trips_analysis_table)
    df = query(db_path, sql)

    # add calculated mean_pace
    df['mean_pace'] = df['total_duration'] / df['total_distance']

    # update dtypes
    df[datetime_col] = pd.to_datetime(df[datetime_col])

    # create table (if not exists)
    sql = """
            CREATE TABLE IF NOT EXISTS {table} (
                rowid INTEGER PRIMARY KEY,
                {datetime_col} TEXT,
                {locationid_col} INTEGER,
                trip_count INTEGER,
                total_duration INTEGER,
                total_distance FLOAT,
                mean_pace FLOAT
            ); """.format(table=table, datetime_col=datetime_col,
                          locationid_col=locationid_col)
    create_table(db_path=db_path, table=table, create_sql=sql, indexes=[],
                 overwrite=overwrite, verbose=verbose)

    # write summary data to table
    df_to_table(db_path, df, table=table, overwrite=False, verbose=verbose)

    if verbose >= 1:
        output('Finished creating or updating {table} table. Dataframe shape '
               'is '.format(table=table) + str(df.shape) + '.')

    return df


def create_taxi_zones(db_path, taxizones_path, title=None, overwrite=False,
                      verbose=0):
    """Creates taxi_zones_[title] and taxi_boroughs_[title] tables in a
    database.

    Parameters
    ----------
    db_path : str
        Path to sqlite database.

    taxizones_path : str
        Path to taxi zone lookup csv file.

    title : str or None
        Defines the suffix of the taxi_zones_[title] and taxi_boroughs_[title]
        tables to be created.

    overwrite : bool
        Boolean data type defining whether or not to overwrite existing tables.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df_zones : dataframe
        taxi_zones_[title] dataframe written to db table.

    df_boroughs : dataframe
        taxi_boroughs_[title] dataframe written to db table.

    Notes
    -----
    """

    table_zones = 'taxi_zones_' + title
    table_boroughs = 'taxi_boroughs_' + title
    if verbose >= 2:
        output('Started creating or updating {table_zones} and '
               '{table_boroughs} tables.'.format(table_zones=table_zones,
                                                 table_boroughs=table_boroughs))

    # load taxi zones data, adjust from text to ids, and rename columns
    df_zones = pd.read_csv(taxizones_path)
    borough_names_dict = {
        'Bronx': 1,
        'Brooklyn': 2,
        'EWR': 3,
        'Manhattan': 4,
        'Queens': 5,
        'Staten Island': 6,
        'Unknown': np.nan
    }
    df_zones.replace(to_replace=borough_names_dict, inplace=True)
    col_names_dict = {
        'LocationID': 'location_id',
        'Borough': 'borough_id',
        'Zone': 'zone_name'
    }
    df_zones.rename(index=str, columns=col_names_dict, inplace=True)

    # create tables (if not exists)
    sql = """
                CREATE TABLE IF NOT EXISTS {table_zones} (
                    location_id INTEGER PRIMARY KEY,
                    zone_name TEXT,
                    service_zone TEXT,
                    borough_id INTEGER
                ); """.format(table_zones=table_zones)
    create_table(db_path=db_path, table=table_zones, create_sql=sql, indexes=[],
                 overwrite=overwrite, verbose=verbose)
    sql = """
                CREATE TABLE IF NOT EXISTS {table_boroughs} (
                    borough_id INTEGER PRIMARY KEY,
                    borough_name TEXT,
                    abbreviation TEXT
                ); """.format(table_boroughs=table_boroughs)
    create_table(db_path=db_path, table=table_boroughs, create_sql=sql,
                 indexes=[], overwrite=overwrite, verbose=verbose)

    # write taxi_zones table
    df_to_table(db_path, df_zones, table=table_zones, overwrite=False,
                verbose=verbose)

    # write taxi_boroughs table
    df_boroughs = pd.DataFrame(list(borough_names_dict.items()), columns=[
        'borough_name', 'borough_id'])
    df_boroughs['abbreviation'] = ['BX', 'BK', 'EWR', 'M', 'Q', 'SI', 'UNK']
    df_boroughs.dropna(axis=0, how='any', inplace=True)
    df_to_table(db_path, df_boroughs, table=table_boroughs, overwrite=False)

    if verbose >= 2:
        output('Finished creating or updating {table_zones} and '
               '{table_boroughs} tables.'.format(table_zones=table_zones,
                                                 table_boroughs=table_boroughs))

    return df_zones, df_boroughs


def create_trips_analysis(db_path, start_datetime, end_datetime, shapefile_path,
                          taxizones_path, title=None, overwrite=False,
                          create_zones_tables=True, verbose=0):
    """Creates a trips_analysis_[title] table in a database for analysis,
    with corresponding taxi_zones_[title] and taxi_boroughs_[title] tables. The
    trips_analysis_[title] table contains filtered data from the trips table
    with location_id values added (based on zones defined in the taxi_zones_[
    title] table) and date/hour columns added.

    Parameters
    ----------
    db_path : str
        Path to sqlite database to create or connect to.

    start_datetime : str or None
        Start of time period to query (inclusive). Specify as datetime string
        with year-month-day and hour:minutes:seconds.
        E.g. '2009-01-25 02:00:00' to start with 2am on January 25th of 2009.
        If None, ignores datetime filter.

    end_datetime : str or None
        End of time period to query (inclusive). Specify as datetime string
        with year-month-day and hour:minutes:seconds. If None, ignores datetime
        filter.

    shapefile_path : str
        Path to shapefile containing location boundaries.

    taxizones_path : str
        Path to taxi zone lookup csv file.

    title : str or None
        Defines the suffix of the trips_analysis_[title] and taxi_zones_[title]
        tables to be created.

    overwrite : bool
        Defines whether or not to overwrite existing taxi_analysis_[title]
        table.

    create_zones_tables : bool
        Defines whether or not to create new taxi_zones_[title] and
        taxi_boroughs_[title] tables.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe
        trips_[title] dataframe written to db table.

    df_zones : dataframe
        taxi_zones_[title] dataframe written to db table.

    df_boroughs : dataframe
        taxi_boroughs_[title] dataframe written to db table.

    Notes
    -----
    This function is expected to be specific to certain analyses and is
    therefore somewhat hard-coded.
    Assumes a trips table exists in the db.
    """

    table = 'trips_analysis_' + title
    if verbose >= 1:
        output('Started creating or updating {table} table for time '
               'period {start} to {end}.'.format(table=table,
                                                 start=start_datetime,
                                                 end=end_datetime))

    # get filtered data
    df = query_trips_filtered(db_path, start_datetime, end_datetime,
                              verbose=verbose)

    # add location_id columns
    df = add_location_id(df, shapefile_path, overwrite=False, verbose=verbose)

    # filter out rows with missing or unknown pickup_location_id or
    # dropoff_location_id (location_id >= 264 is unknown)
    nrows_before = df.shape[0]
    df.dropna(axis=0, how='any', subset=['pickup_location_id',
                                         'dropoff_location_id'], inplace=True)
    df = df[(df['pickup_location_id'] < 264) &
            (df['dropoff_location_id'] < 264)]
    nrows_filtered = nrows_before - df.shape[0]
    if verbose >= 2:
        output('Filtered out {nrows_filtered} rows with missing or unknown'
               'pickup_location_id or dropoff_location_id values.'.format(
                nrows_filtered=nrows_filtered))

    # add date and hour columns
    df = add_date_hour(df, verbose=verbose)

    # create table (if not exists)
    sql = """
            CREATE TABLE IF NOT EXISTS {table} (
                trip_id INTEGER PRIMARY KEY,
                taxi_type INTEGER,
                pickup_datetime TEXT,
                pickup_date TEXT,
                pickup_hour INTEGER,
                dropoff_datetime TEXT,
                dropoff_date TEXT,
                dropoff_hour INTEGER,
                pickup_longitude REAL,
                pickup_latitude REAL,
                pickup_location_id INTEGER,
                dropoff_longitude REAL,
                dropoff_latitude REAL,
                dropoff_location_id INTEGER,
                passenger_count INTEGER,
                trip_distance REAL,
                trip_duration REAL,
                trip_pace REAL,
                trip_straightline REAL,
                trip_windingfactor REAL
            ); """.format(table=table)
    indexes = ['CREATE INDEX IF NOT EXISTS {table}_pickup_datetime ON {table} '
               '(pickup_datetime);'.format(table=table)]
    create_table(db_path=db_path, table=table, create_sql=sql, indexes=indexes,
                 overwrite=overwrite, verbose=verbose)

    # write filtered data to table
    df_to_table(db_path, df, table=table, overwrite=False, verbose=verbose)

    # add additional indexes
    conn = connect_db(db_path)
    c = conn.cursor()
    sql = 'CREATE INDEX IF NOT EXISTS {table}_pudate_puhour_pulocid_dolocid ' \
          'ON {table} (pickup_date, pickup_hour, pickup_location_id, ' \
          'dropoff_location_id);'.format(table=table)
    c.execute(sql)
    conn.commit()
    conn.close()

    # drops and creates new taxi_zones_[title] table if needed
    if create_zones_tables:
        df_zones, df_boroughs = create_taxi_zones(db_path, taxizones_path,
                                                  title=title,
                                                  overwrite=True,
                                                  verbose=verbose)
    else:
        df_zones = None
        df_boroughs = None

    if verbose >= 1:
        output('Finished creating or updating {table} table for time '
               'period {start} to {end}.'.format(table=table,
                                                 start=start_datetime,
                                                 end=end_datetime))

    return df, df_zones, df_boroughs


def points_in_shapefile(lats, lons, shapefile_path, verbose=0):
    """Determines which location_id and zone each lat/lon point belongs to,
    given a shapefile. Assumes points are in WGS84 coordinate system. Assumes
    features have location_id and zone attributes within the properties key
    values of the feature dictionary. Takes first matching zone for each point.

    Parameters
    ----------
    lats : list
        List of latitudes (WGS84).

    lons : list
        List of longitudes (WGS84). Must match shape of lats.

    shapefile_path : str
        Path to shapefile with zone and location_id properties for each field.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    location_ids : list
        List of location_ids (int).

    zones : list
        List of zones (str).

    Notes
    -----
    Code should be able to transform the shapefile into WGS84, but have had
    some trouble with correct transformations using taxi_zones.shp.
    """

    # process shapefile layer
    shapes_wgs84, properties = read_shapefile(shapefile_path)

    # loop through points
    location_ids = []
    zones = []
    for lat, lon in zip(lats, lons):
        zone = None
        location_id = np.nan
        if lat and lon:
            point = geo.Point(lon, lat)

            # check if point is within each shape feature (i.e. taxi zone)
            for shape_wgs84, attribute in zip(shapes_wgs84, properties):
                if shape_wgs84.contains(point):
                    location_id = attribute['LocationID']
                    zone = attribute['zone']
                    break

            if verbose >= 3:
                output('Point is in zone ' + zone + ' with location_id ' +
                       str(location_id) + '.')

        location_ids.append(location_id)
        zones.append(zone)

    if verbose >= 2:
        output('Finished identifying zones of points.')

    return location_ids, zones


def process_heat_map_daily(df, boroughs=None, ignore_routes=None,
                           include_routes=None, verbose=0):
    """ Processes a dataframe for heat map visualization. Processing
    includes converting datetime columns, adding route, filling missing
    route-day combinations with nan, renaming columns (removes '_'),
    converting mean_pace from sec./mile to min./mile, and pivoting for use
    with seaborn.heatmap().

    Parameters
    ----------
    df : dataframe
        Dataframe of daily route summary data.

    boroughs : list or None
        Specifies boroughs to include in full dataframe. Specify as a list of
        strings, e.g. ['Bronx', 'Brooklyn'] or ['BX', 'BK'] if using
        abbreviations. If None, only includes unique boroughs found in
        original dataframe.

    ignore_routes : list or None
        List of routes to ignore. Specify as a list of route strings, e.g.
        ['Bronx-Bronx', 'Bronx-EWR'] or ['BX-BX', 'BX-EWR'].

    include_routes : list or None
        List of routes to include, specified by route_id strings. Cannot
        specify both ignore_routes and include_routes arguments.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df_pivot : dataframe
        Processed dataframe, pivoted for heat map visualization,
        with mean_pace in min./mile.

    df_proc : dataframe
        Processed dataframe, without pivot, with mean_pace in min./mile.

    Notes
    -----
    """

    if ignore_routes and include_routes:
        raise ValueError('Cannot specify both ignore_routes and '
                         'include_routes arguments.')

    # update dtypes
    df['pickup_date'] = pd.to_datetime(df['pickup_date']).dt.date

    # add route string column
    pu_col = 'pickup_borough'
    do_col = 'dropoff_borough'
    df['route'] = [str(pu) + '-' + str(do)
                   for pu, do in zip(list(df[pu_col]), list(df[do_col]))]

    # build full dataframe (all dates and all routes initialized with nans)
    pu_dates = df['pickup_date'].unique()
    max_id = max(list(df[pu_col]) + list(df[do_col]))
    if not boroughs:
        uniq_boroughs = {[df['pickup_borough'].unique(),
                          df['dropoff_borough'].unique()]}
        boroughs = sorted(list(uniq_boroughs))
    df_proc = pd.DataFrame({'pickup_date': [], 'route': [], 'mean_pace': []})

    for pu_date in pu_dates:
        for pu_borough in boroughs:
            routes = [str(pu_borough) + '-' + str(do_borough)
                      for do_borough in boroughs]
            num_rows = len(routes)
            df_temp = pd.DataFrame({'pickup_date': [pu_date] * num_rows,
                                    'route': routes,
                                    'mean_pace': [np.nan] * num_rows})
            df_proc = df_proc.append(df_temp, ignore_index=True)

    # get matching indexes in df_proc of available data in df
    proc_indexes = [df_proc.index[(df_proc['pickup_date'] == pu_date) & (
            df_proc['route'] == route_id)].tolist()[0]
                    for pu_date, route_id in zip(list(df['pickup_date']),
                                                 list(df['route']))]

    # update df_proc with available data in df and convert from sec/mile to
    # min./mile
    df.index = proc_indexes
    df_proc.loc[proc_indexes, ['mean_pace']] = df['mean_pace'] / 60
    df_proc = df_proc.reindex(columns=['pickup_date', 'route', 'mean_pace'])

    # filter to ignore routes specified by ignore_routes or only include routes
    # specified by include_routes
    if ignore_routes:
        df_proc = df_proc[~df_proc['route'].isin(ignore_routes)]
    if include_routes:
        df_proc = df_proc[df_proc['route'].isin(include_routes)]

    # reformat and rename columns
    df_proc['pickup_date'] = df_proc['pickup_date'].apply(lambda x: x.strftime(
        '%m-%d'))
    df_proc = df_proc.rename(index=str, columns={'pickup_date': 'date',
                                                 'mean_pace': 'mean pace'})

    # pivot dataframe for heat map visualization
    df_pivot = df_proc.pivot('route', 'date', 'mean pace')

    if verbose >= 1:
        output('Processed dataframe for heat map visualization. Original '
               'dataframe shape is {original_shape}. Pivoted '
               'dataframe shape is {pivot_shape}. Processed dataframe '
               'shape is {proc_shape}.'.format(original_shape=df.shape,
                                               pivot_shape=df_pivot.shape,
                                               proc_shape=df_proc.shape))

    return df_pivot, df_proc


def query_trips_filtered(db_path, start_datetime, end_datetime, verbose=0):
    """Query and filter the trips table in the nyctlc database.

    Parameters
    ----------
    db_path : str
        Path to sqlite database to create or connect to.

    start_datetime : str
        Start of time period to query (inclusive). Specify as datetime string
        with year-month-day and hour:minutes:seconds.
        E.g. '2009-01-25 02:00:00' to start with 2am on January 25th of 2009.

    end_datetime : str
        End of time period to query (inclusive). Specify as datetime string
        with year-month-day and hour:minutes:seconds.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe
        Dataframe of queried and filtered trips data.

    Notes
    -----
    Many of the filters are based on Donovan et al. (see
    https://github.com/Lab-Work/gpsresilience/blob/master/trip.py)
    """

    if verbose >= 1:
        output('Started querying trips filtered data.')

    # connect to database
    conn = connect_db(db_path)

    # query for records matching various filter settings
    # ignore records outside of specified pickup datetime range
    # ignore records from 2010-08 and 2010-09 (lots of errors)
    # ignore records with passenger_count less than one
    # ignore records with trip_distance too small or too large
    # ignore records with trip_duration too small or too large
    # ignore records with trip_pace too small or too large
    # ignore records with trip_straightline too small or too large
    # ignore records with trip_windingfactor below 0.95 (< 1 to account for
    # gps and rounding errors)
    sql = """
            SELECT trip_id, taxi_type,
                pickup_datetime, dropoff_datetime,
                pickup_longitude, pickup_latitude,
                dropoff_longitude, dropoff_latitude,
                passenger_count, trip_distance, trip_duration, trip_pace,
                trip_straightline, trip_windingfactor
            FROM trips
            WHERE
                (pickup_datetime BETWEEN "{start_datetime}" AND
                    "{end_datetime}")
                AND (pickup_datetime NOT BETWEEN "2010-08-01 00:00:00" AND
                    "2010-09-30 23:59:99")
                AND passenger_count > 0
                AND trip_distance BETWEEN 0.001 AND 20
                AND trip_duration BETWEEN 60 AND 3600
                AND trip_pace BETWEEN 40 AND 3600
                AND trip_straightline BETWEEN 0.001 AND 20
                AND trip_windingfactor > 0.95
          """.format(start_datetime=start_datetime, end_datetime=end_datetime)
    df = pd.read_sql_query(sql, conn, parse_dates={'pickup_datetime': {},
                                                   'dropoff_datetime': {}})
    if verbose >= 1:
        output('Finished querying trips filtered data. Dataframe shape is ' +
               str(df.shape) + '.')

    # ignore records with gps outside of NYC ranges
    #             AND (pickup_latitude BETWEEN 40.6 AND 40.9)
    #             AND (dropoff_latitude BETWEEN 40.6 AND 40.9)
    #             AND (pickup_longitude BETWEEN -74.05 AND -73.7)
    #             AND (dropoff_longitude BETWEEN -74.05 AND -73.7)

    # close database connection
    conn.close()

    return df
