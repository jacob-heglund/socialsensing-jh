# -*- coding: utf-8 -*-
"""
Functions for cleaning mdredze Sandy Twitter dataset.


"""

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from statsmodels.graphics.tsaplots import plot_acf
from twitterinfrastructure.tools import cross_corr, output, query


def create_timeseries_diff(df, col1, col2, zone_col, write_path=None):
    """Creates a dataframe where col1 and col2 columns are replaced by
    first differenced time series.

    Parameters
    ----------
    df : Dataframe
        Dataframe to containing time series data to difference (e.g. from
        create_timeseries). Assumes dataframe is multi-indexed by zone_col and
        timedelta (in hours).

    col1 : str
        Name of column containing first time series.

    col2 : str
        Name of column containing second time series.

    zone_col : str
        Name of zone column: 'zone_id' (nyiso zone), 'location_id' (taxi
        zone), or 'borough' (taxi borough).

    write_path : str or None
        If str, then write a csv of the time series dataframe to the
        specified path. Else, do not write.

    Returns
    -------
    df_diff : dataframe

    Notes
    -----
    """

    # create differenced time series dataframe
    df_diff = pd.DataFrame(columns=[zone_col, 'timedelta',
                                    col1, col2])
    df_diff.set_index([zone_col, 'timedelta'], inplace=True)
    zones = pd.unique(df.index.get_level_values(level=zone_col))
    for zone in zones:
        s_y1 = df[col1].xs(zone, level=0).dropna()
        s_y2 = df[col2].xs(zone, level=0).dropna()
        s_y1.index = pd.to_timedelta(s_y1.index.values, unit='h')
        s_y2.index = pd.to_timedelta(s_y2.index.values, unit='h')

        # difference both timeseries
        s_y1_diff = pd.Series(data=np.diff(s_y1), index=s_y1.index.values[0:-1],
                              name=col1)
        s_y2_diff = pd.Series(data=np.diff(s_y2), index=s_y2.index.values[0:-1],
                              name=col2)
        df_zone = pd.concat([s_y1_diff, s_y2_diff], axis=1)
        df_zone.index.name = 'timedelta'
        df_zone = df_zone.reset_index()
        df_zone[zone_col] = zone
        df_zone = df_zone.set_index([zone_col, 'timedelta'])

        # add zone to differenced dataframe
        df_diff = df_diff.append(df_zone, ignore_index=False, sort='False')

    # save to csv
    if write_path:
        df_csv = df_diff.reset_index()
        df_csv['timedelta'] = df_csv['timedelta'].astype('timedelta64[h]')
        df_csv.to_csv(write_path, index=False)

    return df_diff


def create_timeseries_shift(df, df_max_rho, col1, col2, zone_col,
                            write_path=None):
    """Creates a dataframe where the 2nd time series column is time-shifted.

    Parameters
    ----------
    df : Dataframe
        Dataframe to containing time series data to shift (e.g. from
        create_timeseries). Assumes dataframe is multi-indexed by zone_col and
        timedelta (in hours).

    df_max_rho : Dataframe
        Dataframe containing desired shifts for col2 in a 'max-lag' column,
        indexed by zone_col.

    col1 : str
        Name of column containing first time series (copied).

    col2 : str
        Name of column containing second time series. This is the shifted
        time series, where col2_shifted = col2 + shift.

    zone_col : str
        Name of zone column: 'zone_id' (nyiso zone), 'location_id' (taxi
        zone), or 'borough' (taxi borough).

    write_path : str or None
        If str, then write a csv of the time series dataframe to the
        specified path. Else, do not write.

    Returns
    -------
    df_shift : dataframe

    Notes
    -----
    """

    # create shifted time series dataframe
    df_shift = pd.DataFrame(columns=[zone_col, 'timedelta', col1, col2])
    df_shift.set_index([zone_col, 'timedelta'], inplace=True)
    for zone in df_max_rho.index.values:
        if not np.isnan(df_max_rho.loc[zone, 'max-rho']):
            s_y1 = df[col1].xs(zone, level=0).dropna()
            s_y2 = df[col2].xs(zone, level=0).dropna()
            s_y1.index = pd.to_timedelta(s_y1.index.values, unit='h')
            s_y2.index = pd.to_timedelta(s_y2.index.values, unit='h')

            # shift 2nd time series
            shift = df_max_rho.loc[zone, 'max-shift']
            s_y2_shift = s_y2.shift(1, freq=pd.Timedelta(shift, unit='h'))
            df_zone = pd.concat([s_y1, s_y2_shift], axis=1)
            df_zone.index.name = 'timedelta'
            df_zone = df_zone.reset_index()
            df_zone[zone_col] = zone
            df_zone = df_zone.set_index([zone_col, 'timedelta'])

            # add zone to shifted dataframe
            df_shift = df_shift.append(df_zone, ignore_index=False,
                                       sort='False')

    # save to csv
    if write_path:
        df_csv = df_shift.reset_index()
        df_csv['timedelta'] = df_csv['timedelta'].astype('timedelta64[h]')
        df_csv.to_csv(write_path, index=False)

    return df_shift


def create_timeseries(df, zone_col, min_count, write_path=None, verbose=0):
    """Creates a time series dataframe where each column of df is
    independently linearly interpolated over the total range of timedeltas of
    each zone. Only time series with at least min_count data points are
    included. Assumes the dataframe is indexed by a zone column (zone_col)
    and a timedelta column (e.g. using index_timedelta).

    Parameters
    ----------
    df : Dataframe
        Dataframe to calculate time series from.

    zone_col : str
        Name of zone column: 'zone_id' (nyiso zone), 'location_id' (taxi
        zone), or 'borough' (taxi borough).

    min_count : int
        Minimum number of data points needed to convert to a time series.

    write_path : str or None
        If str, then write a csv of the time series dataframe to the
        specified path. Else, do not write.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df_ts : dataframe

    Notes
    -----
    """

    # loop through zones
    df_ts = pd.DataFrame()
    skipped = []
    zones = pd.unique(df.index.get_level_values(zone_col))
    for zone in zones:
        df_zone = df.xs(zone, level=0)

        # loop through columns (i.e. data to convert to time series)
        y_interps = []
        cols = df_zone.columns.values
        for col in cols:
            s = df_zone[col].dropna()
            if s.count() < min_count:
                skipped.append((zone, col))
            else:
                timedeltas = range(s.index.astype('timedelta64[h]').min(),
                                   s.index.astype('timedelta64[h]').max() + 1)
                y_interp = pd.Series(data=np.interp(
                    timedeltas, s.index.astype('timedelta64[h]'), s.values),
                    index=timedeltas, name=col)
                y_interps.append(y_interp)

        # add interpolated data to dataframe
        if y_interps:
            df_temp = pd.concat(objs=y_interps, axis=1, join='outer')
            df_temp = df_temp.set_index(
                pd.to_timedelta(df_temp.index.values, unit='h'))
            df_temp[zone_col] = zone
            df_temp.set_index(zone_col, append=True, inplace=True)
            df_temp.index.names = ['timedelta', zone_col]
            df_temp = df_temp.reorder_levels([1, 0])
            df_ts = df_ts.append(df_temp, sort=False)

    # save to csv
    if write_path:
        df_csv = df_ts.reset_index()
        df_csv['timedelta'] = df_csv['timedelta'].astype('timedelta64[h]')
        df_csv.to_csv(write_path, index=False)

    if verbose >= 1:
        output('skipped zones for having less than {min_count} data points '
               'in original column data: {skipped}'.format(skipped=skipped,
                                                           min_count=min_count))

    return df_ts


def index_timedelta(df, datetime_ref, datetime_col):
    """Indexes a dataframe on a timedelta calculated from datetime_col
    relative to datetime_ref.

    Parameters
    ----------
    df : Dataframe
        Dataframe to reindex on timedelta.

    datetime_ref : Timestamp
        Reference datetime to calculate timedelta relative to, specified as a
        timezone-aware Pandas Timestamp object. Calculates timedelta as
        datetime_col - datetime_ref.
        e.g. enddate = pd.Timestamp('2012-11-03 00:00:00',
        tz='America/New_York')

    datetime_col : str
        Name of column (or index) containing the datetime data to calculate
        timedelta from.

    Returns
    -------
    df : dataframe

    Notes
    -----
    """

    indexes = df.index.names
    df = df.reset_index()

    # calculate and add timedelta
    df['timedelta'] = df['datetimeNY'] - datetime_ref
    # df['timedelta'] = [int(td.total_seconds() / 3600) for td
    #                    in df['timedelta']]
    # df['timedelta'] = pd.to_timedelta(df['timedelta'], unit='h')

    # drop columns and reindex with datetime_col replaced by timedelta
    df = df.drop([datetime_col], axis=1)
    indexes = ['timedelta' if ind == datetime_col else ind for ind in indexes]
    df = df.set_index(indexes)
    df = df.sort_index(level=0)

    return df


def load_nyctlc_zone(startdate, enddate, trip_type, trip_count_filter,
                     db_path, verbose=0):
    """Query and clean nyctlc dropoff or pickup data for the specified date
    range from a sqlite database, grouped by zone. Assumes the database
    contains a standard_zonedropoff_hour_sandy or
    standard_zonepickup_hour_sandy table created using
    create_standard_zone_hour.

    Parameters
    ----------
    startdate : Timestamp
        Start date to include tweets from (inclusive), specified as a
        timezone-aware Pandas Timestamp object.
        E.g. startdate = pd.Timestamp('2012-10-28 00:00:00',
        tz='America/New_York')

    enddate : Timestamp
        End date to include tweets from (exclusive), specified as a
        timezone-aware Pandas Timestamp object.
        e.g. enddate = pd.Timestamp('2012-11-03 00:00:00',
        tz='America/New_York')

    trip_type : str
        Trip type: 'dropoff' or 'pickup'.

    trip_count_filter : int
        Minimum number of trips required to load a data point.

    db_path : str
        Path to sqlite database containing table.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df_taxi : dataframe

    Notes
    -----
    Sqlite date queries are inclusive for start and end, datetimes in nyctlc
    database are local (i.e. NY timezone).
    """

    df_taxi = load_nyctlc_zone_hour(startdate, enddate, trip_type,
                                    trip_count_filter, db_path, verbose=verbose)

    # remove index, remove columns, and group by zone
    df_taxi = df_taxi.reset_index()
    df_taxi = df_taxi.drop(['datetimeNY'], axis=1)
    df_taxi = df_taxi.groupby(['location_id']).mean()

    if verbose >= 1:
        if trip_type == 'dropoff':
            output('[min, max] taxi pace and trips mean z-score: [' +
                   str(np.nanmin(df_taxi['zpace-drop'])) + ', ' +
                   str(np.nanmax(df_taxi['zpace-drop'])) + '], [' +
                   str(np.nanmin(df_taxi['ztrips-drop'])) + ', ' +
                   str(np.nanmax(df_taxi['ztrips-drop'])) + '].')
        elif trip_type == 'pickup':
            output('[min, max] taxi pace and trips mean z-score: [' +
                   str(np.nanmin(df_taxi['zpace-pick'])) + ', ' +
                   str(np.nanmax(df_taxi['zpace-pick'])) + '], [' +
                   str(np.nanmin(df_taxi['ztrips-pick'])) + ', ' +
                   str(np.nanmax(df_taxi['ztrips-pick'])) + '].')

    return df_taxi


def load_nyctlc_zone_date(startdate, enddate, trip_type, trip_count_filter,
                          db_path, verbose=0):
    """Query and clean nyctlc dropoff or pickup data for the specified date
    range from a sqlite database, grouped by zone and date. Assumes the database
    contains a standard_zonedropoff_hour_sandy or
    standard_zonepickup_hour_sandy table created using
    create_standard_zone_hour.

    Parameters
    ----------
    startdate : Timestamp
        Start date to include tweets from (inclusive), specified as a
        timezone-aware Pandas Timestamp object.
        E.g. startdate = pd.Timestamp('2012-10-28 00:00:00',
        tz='America/New_York')

    enddate : Timestamp
        End date to include tweets from (exclusive), specified as a
        timezone-aware Pandas Timestamp object.
        e.g. enddate = pd.Timestamp('2012-11-03 00:00:00',
        tz='America/New_York')

    trip_type : str
        Trip type: 'dropoff' or 'pickup'.

    trip_count_filter : int
        Minimum number of trips required to load a data point.

    db_path : str
        Path to sqlite database containing table.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df_taxi : dataframe

    Notes
    -----
    Sqlite date queries are inclusive for start and end, datetimes in nyctlc
    database are local (i.e. NY timezone).
    """

    df_taxi = load_nyctlc_zone_hour(startdate, enddate, trip_type,
                                    trip_count_filter, db_path, verbose=verbose)

    # remove index, adjust datetime to date, and group by zone and date
    df_taxi = df_taxi.reset_index()
    df_taxi['datetimeNY'] = pd.to_datetime(df_taxi['datetimeNY']).dt.date
    df_taxi = df_taxi.groupby(['location_id', 'datetimeNY']).mean()

    if verbose >= 1:
        if trip_type == 'dropoff':
            output('[min, max] taxi pace and trips mean z-score: [' +
                   str(np.nanmin(df_taxi['zpace-drop'])) + ', ' +
                   str(np.nanmax(df_taxi['zpace-drop'])) + '], [' +
                   str(np.nanmin(df_taxi['ztrips-drop'])) + ', ' +
                   str(np.nanmax(df_taxi['ztrips-drop'])) + '].')
        elif trip_type == 'pickup':
            output('[min, max] taxi pace and trips mean z-score: [' +
                   str(np.nanmin(df_taxi['zpace-pick'])) + ', ' +
                   str(np.nanmax(df_taxi['zpace-pick'])) + '], [' +
                   str(np.nanmin(df_taxi['ztrips-pick'])) + ', ' +
                   str(np.nanmax(df_taxi['ztrips-pick'])) + '].')

    return df_taxi


def load_nyctlc_zone_hour(startdate, enddate, trip_type, trip_count_filter,
                          db_path, verbose=0):
    """Query and clean nyctlc dropoff or pickup data for the specified date
    range from a sqlite database, grouped by zone and hour. Assumes the
    database contains a standard_zonedropoff_hour_sandy or
    standard_zonepickup_hour_sandy table created using
    create_standard_zone_hour.

    Parameters
    ----------
    startdate : Timestamp
        Start date to include tweets from (inclusive), specified as a
        timezone-aware Pandas Timestamp object.
        E.g. startdate = pd.Timestamp('2012-10-28 00:00:00',
        tz='America/New_York')

    enddate : Timestamp
        End date to include tweets from (exclusive), specified as a
        timezone-aware Pandas Timestamp object.
        e.g. enddate = pd.Timestamp('2012-11-03 00:00:00',
        tz='America/New_York')

    trip_type : str
        Trip type: 'dropoff' or 'pickup'.

    trip_count_filter : int
        Minimum number of trips required to load a data point.

    db_path : str
        Path to sqlite database containing table.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df_taxi : dataframe

    Notes
    -----
    Sqlite date queries are inclusive for start and end, datetimes in nyctlc
    database are local (i.e. NY timezone).
    """

    if verbose >= 1:
        output('Started query.')

    # define trip type
    if trip_type not in ['dropoff', 'pickup']:
        raise ValueError('Invalid trip_type argument: {arg}.'.format(
            arg=trip_type))

    # convert datetimes
    enddate_exclusive = enddate - pd.Timedelta('1 second')
    startdate_sql = startdate.strftime("%Y-%m-%d %H:%M:%S")
    enddate_sql = enddate_exclusive.strftime("%Y-%m-%d %H:%M:%S")

    # load dropoff/pickup data
    sql = """
            SELECT {trip_type}_datetime AS datetimeNY,
                {trip_type}_location_id AS location_id,
                z_mean_pace AS zpace, z_trip_count AS ztrips
            FROM standard_zone{trip_type}_hour_sandy
            WHERE
                trip_count > {trip_count_filter} AND
                {trip_type}_datetime BETWEEN
                "{startdate_sql}" AND "{enddate_sql}"
          ;""".format(trip_count_filter=trip_count_filter,
                      startdate_sql=startdate_sql, enddate_sql=enddate_sql,
                      trip_type=trip_type)
    df_taxi = query(db_path, sql)

    # add columns
    df_taxi['abs-zpace'] = abs(df_taxi['zpace'])
    df_taxi['abs-ztrips'] = abs(df_taxi['ztrips'])

    # convert datetimes
    df_taxi['datetimeNY'] = pd.to_datetime(df_taxi['datetimeNY'])
    df_taxi['datetimeNY'] = [dt.tz_localize(tz='America/New_York') for dt in
                             df_taxi['datetimeNY']]

    # index and sort
    df_taxi = df_taxi.set_index(['location_id', 'datetimeNY'])
    df_taxi = df_taxi.sort_index(level=0)

    if verbose >= 1:
        output('[min, max] taxi datetimeNY (hourly): [' +
               str(min(df_taxi.index.get_level_values(level=1))) + ', ' +
               str(max(df_taxi.index.get_level_values(level=1))) + '].')
        output('[min, max] taxi pace and trips mean z-score (hourly): [' +
               str(np.nanmin(df_taxi['zpace'])) + ', ' +
               str(np.nanmax(df_taxi['zpace'])) + '], [' +
               str(np.nanmin(df_taxi['ztrips'])) + ', ' +
               str(np.nanmax(df_taxi['ztrips'])) + '].')

    # add drop or pick to column names
    if trip_type == 'dropoff':
        val = '-drop'
    elif trip_type == 'pickup':
        val = '-pick'
    else:
        pass
    col_dict = {}
    for col in df_taxi.columns.values:
        col_dict[col] = col + val
    df_taxi = df_taxi.rename(col_dict, axis='columns')

    return df_taxi


def load_nyiso(startdate, enddate, db_path, verbose=0):
    """Query and clean nyiso load forecast error data for the specified date
    range from a sqlite database. Assumes the database contains a
    forecast_error table created using create_forecast_err.

    Parameters
    ----------
    startdate : Timestamp
        Start date to include tweets from (inclusive), specified as a
        timezone-aware Pandas Timestamp object.
        E.g. startdate = pd.Timestamp('2012-10-28 00:00:00',
        tz='America/New_York')

    enddate : Timestamp
        End date to include tweets from (exclusive), specified as a
        timezone-aware Pandas Timestamp object.
        e.g. enddate = pd.Timestamp('2012-11-03 00:00:00',
        tz='America/New_York')

    db_path : str
        Path to sqlite database containing table.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe

    Notes
    -----
    Sqlite date queries are inclusive for start and end, forecast_error
    datetimes are UTC.
    """

    if verbose >= 1:
        output('Started query.')

    # convert datetimes
    startdateUTC = startdate.tz_convert('UTC')
    enddateUTC = enddate.tz_convert('UTC') - pd.Timedelta('1 second')
    startdate_sql = startdateUTC.strftime("%Y-%m-%d %H:%M:%S")
    enddate_sql = enddateUTC.strftime("%Y-%m-%d %H:%M:%S")

    # load nyiso load data
    sql = """
            SELECT datetimeUTC, zone_id AS nyiso_zone,
                forecast_error_p0 AS err0
            FROM forecast_error
            WHERE
                datetimeUTC BETWEEN "{startdate_sql}" AND "{enddate_sql}"
          ;""".format(startdate_sql=startdate_sql, enddate_sql=enddate_sql)
    df = query(db_path, sql)

    # convert datetimes
    df['datetimeUTC'] = pd.to_datetime(df['datetimeUTC'])
    df['datetimeUTC'] = [datetime.tz_localize(tz='UTC') for datetime in
                         df['datetimeUTC']]
    df['datetimeNY'] = [datetime.tz_convert('America/New_York') for
                        datetime in df['datetimeUTC']]

    # add and drop columns
    df['percent-err0'] = df['err0'] * 100
    df = df.drop(['datetimeUTC'], axis=1)

    # index and sort
    df = df.set_index(['nyiso_zone', 'datetimeNY'])
    df = df.sort_index(level=0)

    if verbose >= 1:
        output('[min, max] forecast error datetimeNY: [' +
               str(min(df.index.get_level_values(level=1))) + ', ' +
               str(max(df.index.get_level_values(level=1))) + '].')
        output('[min, max] forecast error: [' +
               str(np.nanmin(df['err0'])) + ', ' +
               str(np.nanmax(df['err0'])) + '].')
        output('Finished query.')

    return df


def max_cross_corr(df, col1, col2, zone_col, shifts, min_overlap, verbose=0):
    """Creates a dataframe containing the time shift that maximizes
    cross-correlation between two time series, the max cross-correlation value,
    and the number of overlapping data points in those series.

    Parameters
    ----------
    df : Dataframe
        Dataframe to containing time series data (e.g. from
        create_timeseries). Assumes dataframe is multi-indexed by zone_col and
        timedelta (in hours).

    col1 : str
        Name of column containing first time series.

    col2 : str
        Name of column containing second time series. This is the shifted
        time series, where col2_shifted = col2 + shift.

    zone_col : str
        Name of spatial zone index.

    shifts : list
        List of time shifts to apply to 2nd time series (in hours).

    min_overlap : int
        Minimum number of overlapping data points (after the 2nd series is time
        shifted) needed to calculate cross-correlation.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df_max_rho : dataframe
        Dataframe of max cross-correlations and associated shifts and counts.

    df_rho : dataframe
        Dataframe of cross-correlations and associated shifts and counts for
        all shifts.

    Notes
    -----
    """

    df_rho = pd.DataFrame(columns=['shift', zone_col, 'rho'])
    df_count = pd.DataFrame(columns=['shift', zone_col, 'count'])
    skipped = []
    zones = pd.unique(df.index.get_level_values(zone_col))
    for shift in shifts:
        for zone in zones:
            s_y1 = df[col1].xs(zone, level=0).dropna()
            s_y2 = df[col2].xs(zone, level=0).dropna()
            s_y1.index = pd.to_timedelta(s_y1.index.values, unit='h')
            s_y2.index = pd.to_timedelta(s_y2.index.values, unit='h')

            # shift 2nd time series
            s_y2_shift = s_y2.shift(1, freq=pd.Timedelta(shift, unit='h'))

            # skip zone if not enough overlapping data points (after shift)
            df_zone = pd.concat([s_y1, s_y2_shift], axis=1).dropna()
            num_overlap = df_zone.shape[0]
            if num_overlap < min_overlap:
                df_rho = df_rho.append({'shift': shift, zone_col: zone,
                                        'rho': np.nan}, ignore_index=True)
                skipped.append((shift, zone))
                continue

            # normalized cross-correlation
            rho = cross_corr(df_zone[col1].values, df_zone[col2].values, True)
            df_rho = df_rho.append({'shift': shift, zone_col: zone, 'rho': rho},
                                   ignore_index=True)
            df_count = df_count.append({'shift': shift, zone_col: zone,
                                        'count': num_overlap},
                                       ignore_index=True)

    # reshape and get max rhos and associated shifts and counts
    df_rho = df_rho.set_index(['shift', zone_col])
    df_rho_reshape = df_rho.reset_index()
    df_rho_reshape = df_rho_reshape.pivot(index='shift', columns=zone_col,
                                          values='rho')
    s_max_shifts = df_rho_reshape.idxmax(axis=0)
    s_max_shifts.name = 'max-shift'
    s_max_rhos = df_rho_reshape.max(axis=0)
    s_max_rhos.name = 'max-rho'
    df_count = df_count.set_index(['shift', zone_col])
    max_counts = []
    for zone in zones:
        max_shift = s_max_shifts.loc[zone]
        if np.isnan(max_shift):
            max_counts.append(np.nan)
        else:
            max_counts.append(df_count.loc[max_shift, zone].item())
    s_max_counts = pd.Series(max_counts, index=zones)
    s_max_counts.name = 'max-count'
    df_max_rho = pd.concat([s_max_rhos, s_max_shifts, s_max_counts], axis=1)

    if verbose >= 2:
        output('Skipped {num_skipped} (shift, {zone}) combos: {skipped}'.format(
            num_skipped=len(skipped), zone=zone_col, skipped=skipped))

    return df_max_rho, df_rho


def plot_acf_series(s, figsize=(6, 4),
                    xlabel='Lag', ylabel=None,
                    save_path=None):
    """Creates a dataframe containing the time shift that maximizes
    cross-correlation between two time series, the max cross-correlation value,
    and the number of overlapping data points in those series.

    Parameters
    ----------
    s : list-like object
        List-like object (e.g. list, pandas series) containing time series to
        plot acf for. If ylable is None, then s must be a labeled series.

    figsize : tuple (optional)
        Two element tuple defining figure size in inches (width, height).

    xlabel : str (optional)
        Defines x-axis label.

    ylabel : str (optioal)
        Defines left y-axis label.

    save_path : str or None
        If str, defines path for saving figure. If None, does not save figure.

    Returns
    -------

    Notes
    -----
    """

    # create figure and add acf plot
    fig, ax = plt.subplots(figsize=figsize, tight_layout=False)
    plot_acf(s, ax=ax)

    # axes
    ax.tick_params(axis='x', colors='k')
    ax.tick_params(axis='y', colors='k')
    ax.set_xlabel(xlabel, color='k')
    if ylabel:
        ax.set_ylabel(ylabel, color='k')
    else:
        ax.set_ylabel('ACF ({var})'.format(var=s.name), color='k')
    plt.title('')

    # save
    if save_path:
        fig.set_size_inches(figsize[0], figsize[1])
        plt.savefig(save_path, dpi=300, bbox_inches='tight')


def plot_timeseries(s1, s2, figsize=(6, 4),
                    linestyles=('-', '--'),
                    colors=('xkcd:black', 'xkcd:red'),
                    xlabel='Timedelta, hours', y1label=None, y2label=None,
                    save_path=None):
    """Creates a dataframe containing the time shift that maximizes
    cross-correlation between two time series, the max cross-correlation value,
    and the number of overlapping data points in those series.

    Parameters
    ----------
    s1 : series
        1st pandas series indexed by timedelta. Series must be named if y1label
        is None.

    s2 : series
        2nd pandas series indexed by timedelta. Series must be named if y2label
        is None.

    figsize : tuple (optional)
        Two element tuple defining figure size in inches (width, height).

    linestyles : tuple (optional)
        Tuple defining line styles to use for each series.

    colors : tuple (optional)
        Tuple defining colors to use for each series.

    xlabel : str
        Defines x-axis label.

    y1label : str
        Defines left y-axis label.

    y2label : str
        Defines right y-axis label.

    save_path : str or None
        If str, defines path for saving figure. If None, does not save figure.

    Returns
    -------

    Notes
    -----
    """

    # get data
    s1.index = pd.to_timedelta(s1.index.values, unit='h')
    s2.index = pd.to_timedelta(s2.index.values, unit='h')

    # create figure
    fig, ax1 = plt.subplots(figsize=figsize, tight_layout=False)
    ax2 = ax1.twinx()
    ax2.grid(None)
    lines = []

    # add shaded line plots
    x1 = [int(td.total_seconds() / 3600) for td in s1.index]
    line = ax1.plot(x1, s1.values,
                    color=colors[0], linestyle=linestyles[0])
    ax1.fill_between(x1, y1=list(s1.values), y2=min(s1.values),
                     color=colors[0], linestyle=linestyles[0],
                     alpha=0.4)
    lines.append(line[0])
    x2 = [int(td.total_seconds() / 3600) for td in s2.index]
    line = ax2.plot(x2, s2.values,
                    color=colors[1], linestyle=linestyles[1])
    ax2.fill_between(x2, y1=s2.values, y2=min(s2.values),
                     color=colors[1], linestyle=linestyles[1],
                     alpha=0.4)
    lines.append(line[0])

    # axes
    ax1.tick_params(axis='x', colors='k')
    ax1.tick_params(axis='y', colors=colors[0])
    ax2.tick_params(axis='y', labelcolor=colors[1])
    ax1.set_xlabel(xlabel, color=colors[0])
    if y1label:
        ax1.set_ylabel(y1label, color=colors[0])
    else:
        ax1.set_ylabel(s1.name, color=colors[0])
    if y2label:
        ax2.set_ylabel(y2label, color=colors[1])
    else:
        ax2.set_ylabel(s1.name, color=colors[1])

    # save
    if save_path:
        fig.set_size_inches(figsize[0], figsize[1])
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
