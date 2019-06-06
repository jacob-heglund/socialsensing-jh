# -*- coding: utf-8 -*-
"""
General functions supporting other modules.


"""

import datetime as dt
import fiona
import numpy as np
import os
import pandas as pd
import pyproj
import sqlite3
from functools import partial
from shapely import geometry as geo
from shapely import ops


def boxcox_backtransform(xt, lmbda):
    """Back transform box-cox transformed data. Assumes data was transformed
    using equation from scipy.stats.box.

    Parameters
    ----------
    xt : array
        Array of box-cox transformed data.

    lmbda : scalar
        Lambda used during box-cox transformation.

    Returns
    -------
    x : array
        Array of back transformed data.

    Notes
    -----
    """

    if lmbda == 0:
        x = np.e ** xt
    else:
        x = [(lmbda * val + 1) ** (1 / lmbda) for val in np.nditer(xt)]

    return x


def check_expected_list(df, col_name, expected_values, verbose=0):
    """Checks that column values match expected.

    Parameters
    ----------
    df : dataframe
        Dataframe to check.

    col_name : str
        Name of column to check.

    expected_values : list
        Expected values for column.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    match : bool
        True if all values match expected; otherwise, False.

    Notes
    -----
    """

    uniq = pd.Series(pd.unique(df[col_name]))
    uniq_in = uniq.isin(expected_values)

    if verbose >= 3:
        output('Unique ' + col_name + ' values: ')
        print(uniq)
        print('')

    match = True
    if not all(uniq_in):
        uniq_not_in = [not i for i in uniq_in]
        output('Error : Unexpected ' + col_name + ' value(s).',
               'check_expected')
        print(uniq[uniq_not_in])
        match = False

    return match


def connect_db(db_path, verbose=0):
    """Connects to a sqlite database. Creates the database if it does not exist.

    Parameters
    ----------
    db_path : str
        Path to sqlite database to create or connect to.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    conn : database connection
        Returns database connection.

    Notes
    -----
    db_path = '/Users/httran/Documents/projects/twitterinfrastructure/data
        /processed/nyctlc-triprecorddata.db'
    """

    conn = sqlite3.connect(db_path)
    if verbose >= 1:
        output('Connected to (or created if not exists) sqlite database.')

    return conn


def create_table(db_path, table, create_sql, indexes=None, overwrite=False,
                 verbose=0):
    """Creates a sqlite table.

    Parameters
    ----------
    db_path : str
        Path to sqlite database to create or connect to.

    table : str
        Name of table to be created.

    create_sql : str
        Sql query, defined as a string.
        E.g. 'CREATE TABLE IF NOT EXISTS table (col1 TEXT, col2 INTEGER);'

    indexes : list or None
        List of create index sql statements, each defined as a string.
        E.g. ['CREATE INDEX IF NOT EXISTS temp ON table col;']

    overwrite : bool
        Defines whether or not to overwrite existing table.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------

    Notes
    -----
    """

    # connect to database
    conn = connect_db(db_path)
    c = conn.cursor()

    # drop table if needed
    if overwrite:
        sql = 'DROP TABLE IF EXISTS {table};'.format(table=table)
        c.execute(sql)
        conn.commit()
        if verbose >= 1:
            output('Dropped {table} table (if exists).'.format(table=table))

    # create table (if not exists)
    c.execute(create_sql)
    conn.commit()

    # create indexes
    if indexes:
        for index_sql in indexes:
            c.execute(index_sql)
            conn.commit()

    # close connection
    conn.close()

    if verbose >= 1:
        output('Created new (if not exists) {table} table.'.format(table=table))


def cross_corr(a, b, normalized=True):
    """Calculate the cross-correlation between two datasets of the same length.

    Parameters
    ----------
    a : array
        1-d array of the first dataset.

    b : array
        1-d array of the second dataset.

    normalized : bool
        If True, then normalize each dataset.

    Returns
    -------
    rho : cross-correlation

    Notes
    -----
    """

    if normalized:
        a = (a - np.mean(a)) / (np.std(a) * len(a))
        b = (b - np.mean(b)) / (np.std(b))
    rho = np.correlate(a, b, mode='valid')[0]

    return rho


def df_to_table(db_path, df, table, dtype={}, overwrite=False, verbose=0):
    """Writes a dataframe to a table in a database.

    Parameters
    ----------
    db_path : str
        Path to sqlite database.

    df : dataframe
        Dataframe to write from.

    table : str
        Name of table in database.

    dtype : dict
        See pandas documentation for to_sql.

    overwrite : bool
        Boolean data type defining whether or not to overwrite existing table.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------

    Notes
    -----
    """

    # connect to database
    conn = connect_db(db_path)
    c = conn.cursor()

    # write to table
    if overwrite:
        df.to_sql(table, conn, if_exists='replace', index=False, dtype=dtype)
        if verbose >= 1:
            output('Wrote dataframe to new {table} table.'.format(table=table))
    else:
        df.to_sql(table, conn, if_exists='append', index=False, dtype=dtype)
        if verbose >= 1:
            output('Wrote dataframe to new (if not exists) or existing {table} '
                   'table.'.format(table=table))
    conn.close()


def dump(items, func_name='unknown', tostr=True, overwrite=True):
    """Dumps a list of items to a text file, where each item is written to a
    new line. Dump file is written to the 'data/dump/' directory with a
    [yearmonthday_hourminuteseconds] timestamp.

    Parameters
    ----------
    items : list
        List of items to dump.

    func_name : str
        Function name to associate with the dump file.

    tostr : bool
        Defines whether or not to convert items to strings.

    overwrite : bool
        Defines whether or not to overwrite existing file.

    Returns
    -------
    written : bool
        Defines whether or not dump file was written.

    Notes
    -----
    """

    written = False
    if items:
        path = 'data/dump/dump-{func_name}-{date:%Y%m%d_%H%M%S}.txt'.format(
            func_name=func_name, date=dt.datetime.now())
        if not tostr:
            items = [str(item) for item in items]
        if overwrite:
            open_str = 'w'
        else:
            open_str = 'a'
        with open(path, open_str) as file:
            for item in items:
                file.write('{}\n'.format(item))
            written = True

    return written


def get_regex_files(files_dir, pattern, verbose=0):
    """Get file names matching regex pattern from specified directory.

    Parameters
    ----------
    files_dir : str
        Path of directory to get file names from.

    pattern : regex
        Regex pattern.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    files : list
        List of matching file names in directory.

    Notes
    -----
    """

    files = [f for f in os.listdir(files_dir) if pattern.match(f)]
    files.sort()

    if verbose >= 1:
        output(str(len(files)) + ' matching files in \"' + files_dir + '\".')

    return files


def haversine(lat1, lon1, lat2, lon2):
    """Calculate the great circle distance between two points on earth (
    specified in decimal degrees). All arguments must be of equal shape.

    Parameters
    ----------
    lat1 : list
        List of latitudes for point 1 (decimal degrees).

    lon1 : list
        List of longitudes for point 1.

    lat2 : list
        List of latitudes for point 2.

    lon2 : list
        List of longitudes for point 2.

    Returns
    -------
    d : list
        List of distances (miles).

    Notes
    -----
    See https://stackoverflow.com/questions/29545704/fast-haversine
    -approximation-python-pandas.
    """

    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    R = 3956.5465  # earth's radius in miles (taken as average between poles
    # and equator)
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(
        dlon / 2)**2
    d = 2 * R * np.arcsin(np.sqrt(a))

    return d


def output(print_str, fn_str=None):
    """Handles print statements with standard format.

    Parameters
    ----------
    print_str : str
        String to print.

    fn_str : str
        Name of function the print statement is being called from.

    Returns
    -------

    Notes
    -----
    """

    if fn_str:
        print(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' : ' +
              fn_str + ' : ' + print_str)
    else:
        print(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") + ' : ' +
              print_str)
    print('')


def read_shapefile(path, to_wgs84=True):
    """Reads a shapefile into lists of shapes and properties for each feature
    within the shapefile layer.

    Parameters
    ----------
    path : str
        Path to shapefile. Assumes the shapefile contains one layer with
        all features of interest. Assumes each feature contains 'geometry'
        and 'properties' attributes.

    to_wgs84 : bool
        If True, applies coordinate transformation to WGS84.

    Returns
    -------
    shapes : list
        List of features as shapely shapes.

    properties : list
        List of feature properties (i.e. attributes).

    Notes
    -----
    """
    # updated fiona version with Python 3 requires explicit GDAL_ENV ignore
    # reads shapefile layer
    with fiona.Env():
        with fiona.open(path, 'r') as fiona_collection:
            # define projection transformation function
            if to_wgs84:
                proj_in = pyproj.Proj(fiona_collection.crs)
                proj_out = pyproj.Proj(init='EPSG:4326')  # WGS84
                proj = partial(pyproj.transform, proj_in, proj_out)

            # save layer as list
            layer = list(fiona_collection)

    # get WGS84 shapes and properties
    shapes = []
    properties = []
    for feature in layer:
        shape = geo.asShape(feature['geometry'])
        if to_wgs84:
            shapes.append(ops.transform(proj, shape))
        else:
            shapes.append(shape)
        properties.append(feature['properties'])

    return shapes, properties


def query(db_path, sql, parse_dates=False, verbose=0):
    """Query a database. Opens and closes database connection.

    Parameters
    ----------
    db_path : str
        Path to sqlite database to create or connect to.

    sql : str
        Sql query.

    parse_dates : dict or False
        Defines which columns to read as datetime dtype.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe
        Dataframe of queried trips data.

    Notes
    -----
    """

    if verbose >= 1:
        output('Started query.')

    # connect to database, query, and close database connection
    conn = connect_db(db_path)
    if parse_dates:
        df = pd.read_sql_query(sql, conn, parse_dates=parse_dates)
    else:
        df = pd.read_sql_query(sql, conn)
    conn.close()

    if verbose >= 1:
        output('Finished query. Dataframe shape is ' + str(df.shape) + '.')

    return df
