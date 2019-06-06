# -*- coding: utf-8 -*-
"""
Functions for importing nyc tlc data.


"""

import numpy as np
import pandas as pd
import re
from twitterinfrastructure.tools import check_expected_list, create_table, \
    connect_db, df_to_table, get_regex_files, haversine, output
from urllib.request import urlretrieve


def add_trip_columns(df, verbose=0):
    """Adds calculated trip columns to the dataframe. Assumes the dataframe
    has already been cleaned. Also removes any trips with unreasonable
    values. Can only calculate distance-related trip data for records with
    pickup/dropoff lat/lon data.

    Parameters
    ----------
    df : dataframe
        Dataframe to add trip calculation columns to.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe
        Dataframe with added columns.

    Notes
    -----
    """

    col_names = list(pd.Series(df.columns.values))

    # add trip_duration column
    if ('dropoff_datetime' in col_names) and ('pickup_datetime' in col_names):
        df['trip_duration'] = (df['dropoff_datetime'] - df['pickup_datetime']) \
                              / np.timedelta64(1, 's')
        if verbose >= 2:
            output('Finished adding trip duration column.')
    elif verbose >= 2:
        output('Unable to add trip_duration column due to missing columns.')

    # add calculated trip columns
    if ('pickup_longitude' in col_names) and \
       ('pickup_latitude' in col_names) and \
       ('dropoff_longitude' in col_names) and \
       ('dropoff_latitude' in col_names):

        # add trip_pace column
        df['trip_distance'].replace(0, np.nan, inplace=True)
        df['trip_pace'] = df['trip_duration'] / df['trip_distance']

        # add trip_straightline_distance column
        df['trip_straightline'] = haversine(df['pickup_latitude'],
                                            df['pickup_longitude'],
                                            df['dropoff_latitude'],
                                            df['dropoff_longitude'])

        # add trip_windingfactor column
        df['trip_windingfactor'] = df['trip_distance'] / df['trip_straightline']

        if verbose >= 2:
            output('Finished adding calculated trip columns.')
    elif verbose >= 2:
        output('Unable to add calculated trip columns due to missing columns.')

    return df


def clean_column_names(df, year, verbose=0):
    """Cleans the dataframe column names. Column names are loosely based on
    "data_dictionary_trip_records_yellow.pdf".

    Parameters
    ----------
    df : dataframe
        Dataframe to clean.

    year : int
        Year data comes from.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe
        Dataframe with cleaned column.

    Notes
    -----
    """

    # update column names
    df = df.rename(index=str, columns=col_names_dict(year))
    if verbose >= 2:
        output('Finished re-naming columns.')

    # add taxi_type column (2 for yellow)
    df.insert(0, 'taxi_type', 2)

    # check that column names match expected
    expected_names = ['taxi_type', 'vendor_id', 'pickup_datetime',
                      'dropoff_datetime', 'passenger_count', 'trip_distance',
                      'pickup_longitude', 'pickup_latitude',
                      'pickup_location_id', 'rate_code_id',
                      'store_and_fwd_flag', 'dropoff_longitude',
                      'dropoff_latitude', 'dropoff_location_id',
                      'payment_type', 'fare_amount', 'extra', 'mta_tax',
                      'improvement_surcharge', 'tip_amount', 'tolls_amount',
                      'total_amount']
    col_names = pd.Series(df.columns.values)
    col_names_in = col_names.isin(expected_names)
    if verbose >= 3:
        output('Column names: ')
        print(col_names)
        print('')
    if not all(col_names_in):
        col_names_not_in = [not i for i in col_names_in]
        output('Error : Unexpected column name(s).', 'clean_column_names')
        print(col_names[col_names_not_in])
        raise ValueError('Unexpected column name(s).')

    return df


def clean_datetime(df, year, month, verbose=0):
    """Cleans the datetime columns. Cleaning involves adjusting data type to
    datetime and removing records outside of expected year and month.

    Parameters
    ----------
    df : dataframe
        Dataframe to clean.

    year : int
        Year data comes from.

    month : int
        Month data comes from.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe
        Dataframe with cleaned column.

    nrows_removed : int
        Number of removed rows.

    Notes
    -----
    """

    col_names = list(pd.Series(df.columns.values))
    nrows_removed = 0
    if ('pickup_datetime' in col_names) and ('dropoff_datetime' in col_names):

        # change datetime columns datetime data type and sort
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
        df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])
        df.sort_values(['pickup_datetime', 'dropoff_datetime'], inplace=True)
        if verbose >= 2:
            output('Finished converting datetime columns to datetime dtype '
                   'and sorting by pickup_datetime and dropoff_datetime.')

        # remove rows outside of expected year-month (based on pickup_datetime)
        start_datetime = pd.datetime(year=year, month=month, day=1,
                                     hour=0, minute=0, second=0, microsecond=0)
        if month < 12:
            end_datetime = pd.datetime(year=year, month=(month + 1), day=1,
                                       hour=0, minute=0, second=0,
                                       microsecond=0)
        else:
            end_datetime = pd.datetime(year=(year + 1), month=1, day=1,
                                       hour=0, minute=0, second=0,
                                       microsecond=0)
        correct_month = (df['pickup_datetime'] >= start_datetime) & \
                        (df['pickup_datetime'] < end_datetime)
        if not all(correct_month):
            nrows = df.shape[0]
            df = df[correct_month]
            nrows_removed = nrows - df.shape[0]
        if verbose >= 2:
            output('Finished removing records with pickup_datetime outside of '
                   'expected year-month date range (' + str(nrows_removed) +
                   ' rows removed).')
    elif verbose >= 1:
        output('Unable to clean datetime columns due to missing columns.')

    return df, nrows_removed


def clean_lat_lon(df, verbose=0):
    """Cleans the latitude and longitude columns.

    Parameters
    ----------
    df : dataframe
        Dataframe to clean.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe
        Dataframe with cleaned column.

    Notes
    -----
    """

    col_names = list(pd.Series(df.columns.values))
    if ('pickup_longitude' in col_names) and \
       ('pickup_latitude' in col_names) and \
       ('dropoff_longitude' in col_names) and \
       ('dropoff_latitude' in col_names):

        # replace lat/lon outside of possible ranges with nan
        df.loc[abs(df['pickup_latitude']) > 90, 'pickup_latitude'] = np.nan
        df.loc[abs(df['dropoff_latitude']) > 90, 'dropoff_latitude'] = np.nan
        df.loc[abs(df['pickup_longitude']) > 180, 'pickup_longitude'] = np.nan
        df.loc[abs(df['dropoff_longitude']) > 180, 'dropoff_longitude'] = np.nan
        if verbose >= 2:
            output('Finished replacing lat/lon outside of possible ranges with '
                   'nan.')

    elif verbose >= 1:
        output('Unable to clean lat/lon columns due to missing columns.')

    return df


def clean_payment_type(df, verbose=0):
    """Cleans the payment_type column.

    Parameters
    ----------
    df : dataframe
        Dataframe to clean.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe
        Dataframe with cleaned column.

    Notes
    -----
    """

    col_names = list(pd.Series(df.columns.values))
    if 'payment_type' in col_names:

        # replace payment_type values with IDs
        payment_str = 'payment_type'
        df[payment_str] = df[payment_str].replace(['Credit', 'CREDIT', 'CRE',
                                                   'Cre', 'CRD'], '1')
        df[payment_str] = df[payment_str].replace(['CASH', 'Cash', 'CAS',
                                                   'Cas', 'CSH'], '2')
        df[payment_str] = df[payment_str].replace(['No', 'No ', 'No Charge',
                                                   'NOC'], '3')
        df[payment_str] = df[payment_str].replace(['Dis', 'DIS', 'Dispute'],
                                                  '4')
        df[payment_str] = df[payment_str].replace(['UNK', 'C', 'NA', 'NA '],
                                                  '5')
        df[payment_str] = df[payment_str].replace(['Voided trip'], '6')
        df[payment_str] = df[payment_str].astype('int')
        if verbose >= 2:
            output('Finished replacing ' + payment_str + ' with IDs.')

        # check that values match expected
        expected_values = [1, 2, 3, 4, 5, 6]
        match = check_expected_list(df, payment_str, expected_values,
                                    verbose=verbose)
        if not match:
            raise ValueError('Unexpected ' + payment_str + ' value(s).')

    elif verbose >= 2:
        output('Unable to clean payment_type column due to missing column.')

    return df


def clean_store_and_fwd_flag(df, verbose=0):
    """Cleans the store_and_fwd_flag column.

    Parameters
    ----------
    df : dataframe
        Dataframe to clean.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe
        Dataframe with cleaned column.

    Notes
    -----
    """

    col_names = list(pd.Series(df.columns.values))
    if 'store_and_fwd_flag' in col_names:

        # replace store_and_fwd_flag values with IDs
        store_str = 'store_and_fwd_flag'
        df[store_str] = df[store_str].replace(r'\s+', np.nan, regex=True)
        df[store_str] = df[store_str].replace(['*', '2', 2], np.nan)
        df[store_str] = df[store_str].replace(['N', '0'], 0)
        df[store_str] = df[store_str].replace(['Y', '1'], 1)
        df[store_str] = df[store_str].astype('float')
        df[store_str] = df[store_str].round()
        if verbose >= 2:
            output('Finished replacing ' + store_str + ' with IDs.')

        # check that values match expected
        expected_values = [0, 1, np.nan]
        match = check_expected_list(df, store_str, expected_values, verbose=verbose)
        if not match:
            raise ValueError('Unexpected ' + store_str + ' value(s).')

    elif verbose >= 2:
        output('Unable to clean store_and_fwd_flag column due to missing '
               'column.')

    return df


def clean_vendor_id(df, verbose=0):
    """Cleans the vendor_id column.

    Parameters
    ----------
    df : dataframe
        Dataframe to clean.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe
        Dataframe with cleaned column.

    Notes
    -----
    """

    col_names = list(pd.Series(df.columns.values))
    if 'vendor_id' in col_names:

        # replace vendor_id values with IDs
        vendor_str = 'vendor_id'
        df[vendor_str] = df[vendor_str].replace('CMT', '1')
        df[vendor_str] = df[vendor_str].replace('DDS', '3')
        df[vendor_str] = df[vendor_str].replace('VTS', '4')
        df[vendor_str] = df[vendor_str].astype('int')
        if verbose >= 2:
            output('Finished replacing ' + vendor_str + ' with IDs.')

        # check that values match expected
        expected_values = [1, 2, 3, 4]
        match = check_expected_list(df, vendor_str, expected_values,
                                    verbose=verbose)
        if not match:
            raise ValueError('Unexpected ' + vendor_str + ' value(s).')

    elif verbose >= 2:
        output('Unable to clean vendor_id column due to missing column.')

    return df


def clean_yellow(df, year, month, verbose=0):
    """Cleans a dataframe of NYC TLC yellow taxi record data. Assumes all
    data is from the same year.

    Cleaning involves:
        - updating column names and adding taxi_type column
        - replacing vendor_id values with IDs
        - replacing store_and_fwd_flag values with IDs
        - replacing payment_type values with IDs
        - replacing lat/lon values outside of possible ranges with nans

    Parameters
    ----------
    df : dataframe
        Dataframe to clean.

    year : int
        Year data comes from.

    month : int
        Month data comes from.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe
        Cleaned dataframe.

    Notes
    -----
    vendor_id = {1: ['CMT', 'Creative Mobile Technologies, LLC'], 2: 'VeriFone
    Inc.', 3: 'DDS', 4: 'VTS'}
    """

    if verbose >= 1:
        output('Started cleaning dataframe for ' + str(year) + '-' +
               str(month) + '. ')
    nrows_removed = 0

    # clean column names
    df = clean_column_names(df, year, verbose)

    # clean datetime columns
    df, nrows_removed_datetime = clean_datetime(df, year, month, verbose)
    nrows_removed += nrows_removed_datetime

    # clean vendor_id column
    df = clean_vendor_id(df, verbose)

    # clean store_and_fwd_flag column
    df = clean_store_and_fwd_flag(df, verbose)

    # clean payment_type column
    df = clean_payment_type(df, verbose)

    # clean lat/lon columns and add calculated trip columns
    df = clean_lat_lon(df, verbose)
    df = add_trip_columns(df, verbose)

    if verbose >= 1:
        output('Cleaned dataframe for ' + str(year) + '-' + str(month) + '. ' +
               str(nrows_removed) + ' rows removed due to errors during clean.')

    return df


def col_names_dict(year):
    """Returns a dictionary mapping column names for specified year to
    expected column names (i.e. those used in trips table).

    Parameters
    ----------
    year : int
        Year to define keys in column names dictionary.

    Returns
    -------
    col_dict : dict
        Dictionary mapping column names for specified year to expected.

    Notes
    -----
    """

    if year == 2009:
        col_dict = {
            'vendor_name': 'vendor_id',
            'Trip_Pickup_DateTime': 'pickup_datetime',
            'Trip_Dropoff_DateTime': 'dropoff_datetime',
            'Passenger_Count': 'passenger_count',
            'Trip_Distance': 'trip_distance',
            'Start_Lon': 'pickup_longitude',
            'Start_Lat': 'pickup_latitude',
            'Rate_Code': 'rate_code_id',
            'store_and_forward': 'store_and_fwd_flag',
            'End_Lon': 'dropoff_longitude',
            'End_Lat': 'dropoff_latitude',
            'Payment_Type': 'payment_type',
            'Fare_Amt': 'fare_amount',
            'surcharge': 'extra',
            'mta_tax': 'mta_tax',
            'Tip_Amt': 'tip_amount',
            'Tolls_Amt': 'tolls_amount',
            'Total_Amt': 'total_amount'
        }
    elif 2010 <= year <= 2013:
        col_dict = {
            'vendor_id': 'vendor_id',
            'pickup_datetime': 'pickup_datetime',
            'dropoff_datetime': 'dropoff_datetime',
            'passenger_count': 'passenger_count',
            'trip_distance': 'trip_distance',
            'pickup_longitude': 'pickup_longitude',
            'pickup_latitude': 'pickup_latitude',
            'rate_code': 'rate_code_id',
            'store_and_fwd_flag': 'store_and_fwd_flag',
            'dropoff_longitude': 'dropoff_longitude',
            'dropoff_latitude': 'dropoff_latitude',
            'payment_type': 'payment_type',
            'fare_amount': 'fare_amount',
            'surcharge': 'extra',
            'mta_tax': 'mta_tax',
            'tip_amount': 'tip_amount',
            'tolls_amount': 'tolls_amount',
            'total_amount': 'total_amount'
        }
    elif year == 2014:
        col_dict = {
            'vendor_id': 'vendor_id',
            ' pickup_datetime': 'pickup_datetime',
            ' dropoff_datetime': 'dropoff_datetime',
            ' passenger_count': 'passenger_count',
            ' trip_distance': 'trip_distance',
            ' pickup_longitude': 'pickup_longitude',
            ' pickup_latitude': 'pickup_latitude',
            ' rate_code': 'rate_code_id',
            ' store_and_fwd_flag': 'store_and_fwd_flag',
            ' dropoff_longitude': 'dropoff_longitude',
            ' dropoff_latitude': 'dropoff_latitude',
            ' payment_type': 'payment_type',
            ' fare_amount': 'fare_amount',
            ' surcharge': 'extra',
            ' mta_tax': 'mta_tax',
            ' tip_amount': 'tip_amount',
            ' tolls_amount': 'tolls_amount',
            ' total_amount': 'total_amount'
        }
    elif 2015 <= year <= 2016:
        col_dict = {
            'VendorID': 'vendor_id',
            'tpep_pickup_datetime': 'pickup_datetime',
            'tpep_dropoff_datetime': 'dropoff_datetime',
            'passenger_count': 'passenger_count',
            'trip_distance': 'trip_distance',
            'pickup_longitude': 'pickup_longitude',
            'pickup_latitude': 'pickup_latitude',
            'RatecodeID': 'rate_code_id',
            'store_and_fwd_flag': 'store_and_fwd_flag',
            'dropoff_longitude': 'dropoff_longitude',
            'dropoff_latitude': 'dropoff_latitude',
            'payment_type': 'payment_type',
            'fare_amount': 'fare_amount',
            'extra': 'extra',
            'mta_tax': 'mta_tax',
            'tip_amount': 'tip_amount',
            'tolls_amount': 'tolls_amount',
            'improvement_surcharge': 'improvement_surcharge',
            'total_amount': 'total_amount'
        }
    elif year == 2017:
        col_dict = {
            'VendorID': 'vendor_id',
            'tpep_pickup_datetime': 'pickup_datetime',
            'tpep_dropoff_datetime': 'dropoff_datetime',
            'passenger_count': 'passenger_count',
            'trip_distance': 'trip_distance',
            'RatecodeID': 'rate_code_id',
            'store_and_forward': 'store_and_fwd_flag',
            'PULocationID': 'pickup_location_id',
            'DOLocationID': 'dropoff_location_id',
            'payment_type': 'payment_type',
            'fare_amount': 'fare_amount',
            'extra': 'extra',
            'mta_tax': 'mta_tax',
            'tip_amount': 'tip_amount',
            'tolls_amount': 'tolls_amount',
            'improvement_surcharge': 'improvement_surcharge',
            'total_amount': 'total_amount'
        }
    else:
        output('Error : Unexpected year (' + str(year) + ').', 'col_names_dict')
        raise ValueError('Unexpected year.')

    return col_dict


def dl_urls(url_path, dl_dir, taxi_type='all', verbose=0):
    """Downloads NYC TLC taxi record files for the specified taxi type into the
    specified directory, based on a text file containing urls.

    Parameters
    ----------
    url_path : str or None
        Path to text file containing NYC TLC taxi record file urls to
        download from. Does nothing if None.

    dl_dir : str
        Path of directory to download files to.

    taxi_type : str
        Taxi type to create regex for. Use None for all (fhv, green,
        and yellow).

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    dl_num : int
        Number of files downloaded.

    Notes
    -----
    url_path = '/Users/httran/Documents/projects/twitterinfrastructure/data
        /raw/nyctlc-triprecorddata/raw_data_urls.txt'
    dl_dir = '/Users/httran/Documents/projects/twitterinfrastructure/data/raw
        /nyctlc-triprecorddata/data/'
    """

    if not url_path:
        return

    if verbose >= 1:
        output('Started downloading taxi record files from ' +
               url_path + ' to ' + dl_dir)

    # get existing files in directory
    files = get_regex_files(dl_dir,
                            pattern=taxi_regex_patterns(taxi_type='all'))

    # get urls
    df_urls = pd.read_table(url_path, header=None, names=['url'])
    urls = df_urls.as_matrix()

    # download files for specified taxi type (skip already existing ones)
    dl_num = 0
    pattern = taxi_regex_patterns(taxi_type)
    for url in urls:
        parts = url[0].split('/')
        fname = parts[-1]
        if pattern.match(fname) and (fname not in files):
            urlretrieve(url[0], dl_dir + fname)
            output('downloaded: ' + fname)
            dl_num += 1

    if verbose >= 1:
        output('Downloaded ' + str(dl_num) + ' taxi record files from ' +
               url_path + ' to ' + dl_dir)

    return dl_num


def import_trips(url_path, dl_dir, db_path, taxi_type, nrows=None, usecols=None,
                 overwrite=False, verbose=0):
    """Downloads, cleans, and imports nyc tlc taxi record files for the
    specified taxi type into a sqlite database.

    Parameters
    ----------
    url_path : str or None
        Path to text file containing nyc tlc taxi record file urls to
        download from. Set to None to skip download.

    dl_dir : str
        Path of directory to download files to or load files from.

    db_path : str
        Path to sqlite database.

    taxi_type : str
        Taxi type to create regex for ('fhv', 'green', 'yellow', or 'all').

    nrows : int or None
        Number of rows to read. Set to None to read all rows.

    usecols : list
        List of column names to include. Specify columns names as strings.
        Column names can be entered based on names found in original tables
        for the year specified or names found in the trips table. Set to None to
        read all columns.

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

    # download taxi record files
    if url_path:
        dl_num = dl_urls(url_path, dl_dir, taxi_type, verbose=verbose)
    else:
        dl_num = 0

    # get taxi record files
    files = get_regex_files(dl_dir, taxi_regex_patterns(taxi_type),
                            verbose=verbose)

    # create trips table (if needed)
    create_sql = """
                CREATE TABLE IF NOT EXISTS trips (
                    trip_id INTEGER PRIMARY KEY,
                    taxi_type INTEGER,
                    vendor_id INTEGER,
                    pickup_datetime TEXT,
                    dropoff_datetime TEXT,
                    passenger_count INTEGER,
                    trip_distance REAL,
                    pickup_longitude REAL,
                    pickup_latitude REAL,
                    pickup_location_id INTEGER,
                    dropoff_longitude REAL,
                    dropoff_latitude REAL,
                    dropoff_location_id INTEGER,
                    trip_duration REAL,
                    trip_pace REAL,
                    trip_straightline REAL,
                    trip_windingfactor REAL
                ); """
    indexes = ['CREATE INDEX IF NOT EXISTS trips_pickup_datetime ON trips '
               '(pickup_datetime);']
    create_table(db_path, 'trips', create_sql, indexes=indexes,
                 overwrite=overwrite, verbose=verbose)

    # load, clean, and import taxi files into table
    import_num = 0
    for file in files:
        if verbose >= 1:
            output('Started importing ' + file + '.')
        if taxi_type == 'fhv':
            df = pd.DataFrame({'taxi_type': []})
        elif taxi_type == 'green':
            df = pd.DataFrame({'taxi_type': []})
        elif taxi_type == 'yellow':
            df, year, month = load_yellow(dl_dir + file, nrows=nrows,
                                          usecols=usecols, verbose=verbose)
            df = clean_yellow(df, year, month, verbose=verbose)
            import_num += 1
        else:
            output('Unknown taxi_type.', fn_str='import_trips')
            df = pd.DataFrame({'taxi_type': []})

        df_to_table(db_path, df, table='trips', overwrite=False,
                    verbose=verbose)
        if verbose >= 1:
            output('Imported ' + file + '.')
    output('Finished importing ' + str(import_num) + ' files.')

    return dl_num, import_num


def load_yellow(path, nrows=None, usecols=None, verbose=0):
    """Loads an NYC TLC yellow taxi record file (one month of data) into a
    dataframe.

    Parameters
    ----------
    path : str
        Path to NYC TLC taxi record file to load.

    nrows : int or None
        Number of rows to read. Set to None to read all rows.

    usecols : list
        List of column names to include. Specify columns names as strings.
        Column names can be entered based on names found in original tables
        for the year specified or names found in the trips table. Set to None to
        read all columns.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe
        Dataframe of one month of cleaned yellow taxi data.

    year : int
        Year data is from.

    month : int
        Month data is from.

    Notes
    -----
    path = '/Users/httran/Documents/projects/twitterinfrastructure/data/raw
    /nyctlc-triprecorddata/data/yellow_tripdata_2012-01.csv'
    """

    if verbose >= 1:
        output('Started loading to dataframe: ' + path + '.')

    parts = re.split('[/_-]', path)
    year = int(parts[-2])

    parts2 = parts[-1].split('.')
    month = int(parts2[0])

    # adjusts usecols to correctly map to column names for the year data is from
    if usecols:
        col_dict = col_names_dict(year)
        usecols_year = []
        for col in usecols:
            if col in col_dict:
                usecols_year.append(col)
            else:
                col_name_year = [key for key, val in col_dict.items()
                                 if val == col]
                if col_name_year:
                    usecols_year.append(col_name_year[0])
                elif verbose > 1:
                    output('No matching usecols column name "' + col + '" for '
                           'year ' + str(year) + '.', 'load_yellow')
                else:
                    pass
    else:
        usecols_year = usecols

    # read file into dataframe
    df = pd.read_csv(path, nrows=nrows, usecols=usecols_year,
                     error_bad_lines=False,
                     warn_bad_lines=False)
    if verbose >= 1:
        output('Finished loading to dataframe: ' + path + '.')

    return df, year, month


def taxi_regex_patterns(taxi_type='all'):
    """Creates a regex pattern for specified taxi type.

    Parameters
    ----------
    taxi_type : str
        Taxi type to create regex for (fhv, green, yellow, or all).

    Returns
    -------
    pattern : regex
        Regex pattern for specified taxi type.

    Notes
    -----
    """

    # define taxi type regex pattern
    if taxi_type == 'fhv':
        pattern = re.compile('fhv_tripdata_.+.csv')
    elif taxi_type == 'green':
        pattern = re.compile('green_tripdata_.+.csv')
    elif taxi_type == 'yellow':
        pattern = re.compile('yellow_tripdata_.+.csv')
    elif taxi_type == 'all':
        pattern = re.compile('(fhv|green|yellow)\_tripdata_.+.csv')
    else:
        output('Unknown taxi_type.', fn_str='regex_pattern')
        return None

    return pattern
