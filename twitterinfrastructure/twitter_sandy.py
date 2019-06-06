# -*- coding: utf-8 -*-
"""
Functions for cleaning mdredze Sandy Twitter dataset.


"""

import datetime as dt
import json
import nltk
import numpy as np
import pandas as pd
import pymongo
import string
from tqdm import tqdm_notebook as tqdm
from twitterinfrastructure.tools import dump, output


def create_analysis(collection='tweets_analysis',
                    tweet_collection='tweets',
                    nyisozones_collection='nyiso_zones',
                    taxizones_collection='taxi_zones',
                    fields=None,
                    db_name='sandy',
                    db_instance='mongodb://localhost:27017/',
                    progressbar=False, overwrite=False, verbose=0):
    """Creates a collection of tweets for analysis, queried and processed
    from an existing collection of tweets. Assumes the specified mongodb
    instance is already running.

    Parameters
    ----------
    collection : str
        Name of collection to insert analysis tweets into.

    tweet_collection : str
        Name of collection to query tweets from.

    nyisozones_collection : str
        Name of collection to query nyiso load zones.

    taxizones_collection : str
        Name of collection to query taxi zones and boroughs from.

    fields : list or None
        List of tweet field names to keep. If None, keeps all fields.

    db_name : str
        Name of database to connect to.

    db_instance : str
        Mongodb instance to connect to in URI format.

    progressbar : bool
        If True, displays progress bar. Progress bar does not work when
        called from a notebook in PyCharm.

    overwrite : bool
        Defines whether or not to overwrite existing collection.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    insert_num : int
        Number of tweets inserted into collection.

    tokens : set
        Set of tokens in collection.

    Notes
    -----
    fields = ['_id', 'coordinates', 'created_at', 'entities', 'text',
    'id_str', 'place']

    Use 'text' instead of 'full_text' for older tweet datasets.

    Some tweets match multiple taxi zones, causing some (likely all) of the
    failed insert tweets found in dump file.

    MongoDB Compass shows datetime field in current timezone, but that is
    stored as UTC (i.e. retrieving with pymongo correctly displays/converts
    datetime field as UTC).

    Requires download of nltk data (tested with popular package). See
    https://www.nltk.org/data.html for download details.

    Start a mongodb instance by running `$ mongod` from terminal (see
    http://api.mongodb.com/python/current/tutorial.html for more details)
    """

    if verbose >= 1:
        output('Started querying, processing, and inserting tweets from '
               '{tweet_collection} into {collection} collection in {db_name} '
               'database.'.format(tweet_collection=tweet_collection,
                                  collection=collection, db_name=db_name))

    # connect to db (creates if not exists)
    client = pymongo.MongoClient(db_instance)
    db = client[db_name]

    # ensure that nyisozones_collection and taxizones_collection exist
    collections = db.collection_names()
    if (nyisozones_collection not in collections) or \
            (taxizones_collection not in collections):
        output('{nyiso} or {taxi} collection not in database. No action '
               'taken.'.format(nyiso=nyisozones_collection,
                               taxi=taxizones_collection))
        return None, None

    # overwrite collection if needed
    if overwrite:
        db.drop_collection(collection)
        if verbose >= 1:
            output('Dropped {collection} collection (if exists).'.format(
                collection=collection))

    # query, process, and insert analysis tweets
    insert_num = 0
    tokens = set()
    fails = []
    tknzr = nltk.tokenize.TweetTokenizer(strip_handles=True, reduce_len=True)
    stop_list = nltk.corpus.stopwords.words("english") + list(
        string.punctuation)
    stemmer = nltk.stem.PorterStemmer()
    # if progressbar:
    #     zones_iter = tqdm(taxi_zones, total=taxi_zones.count(),
    #                       desc='taxi_zones')
    # else:
    #     zones_iter = taxi_zones
    # for taxi_zone in zones_iter:
    #     # query tweets within current taxi zone
    #     query_dict = {
    #         "coordinates": {
    #             "$geoWithin": {
    #                 "$geometry": taxi_zone['geometry']
    #             }
    #         }
    #     }
    #     full_tweets = db[tweet_collection].find(query_dict)

    # process and insert tweets
    full_tweets = db[tweet_collection].find()
    if progressbar:
        tweets_iter = tqdm(full_tweets, total=full_tweets.count(),
                           desc='tweets', leave=False)
    else:
        tweets_iter = full_tweets
    for full_tweet in tweets_iter:
        # remove extra fields
        if fields:
            tweet = {field: full_tweet[field] for field in fields}
        else:
            tweet = full_tweet

        # identify and add nyiso zone, taxi zone, and taxi borough
        if tweet['coordinates'] is not None:
            query_dict = {
                "geometry": {
                    "$geoIntersects": {
                        "$geometry": tweet['coordinates']
                    }
                }
            }
            nyiso_zone = db[nyisozones_collection].find_one(query_dict)
            if nyiso_zone:
                tweet['nyiso_zone'] = nyiso_zone['properties']['Zone']
            else:
                tweet['nyiso_zone'] = np.nan

            query_dict = {
                "geometry": {
                    "$geoIntersects": {
                        "$geometry": tweet['coordinates']
                    }
                }
            }
            taxi_zone = db[taxizones_collection].find_one(query_dict)
            if taxi_zone:
                tweet['location_id'] = taxi_zone['properties']['LocationID']
                tweet['borough'] = taxi_zone['properties']['borough']
            else:
                tweet['location_id'] = np.nan
                tweet['borough'] = np.nan
        else:
            fails.append(tweet['id_str'])
            if verbose >= 2:
                output('Tweet skipped due to missing coordinates.',
                       'create_analysis')
            continue

        # skip tweets missing nyiso and taxi zone
        if (tweet['nyiso_zone'] is np.nan) and (tweet['location_id'] is np.nan):
            fails.append(tweet['id_str'])
            if verbose >= 2:
                output('Tweet skipped due to missing nyiso or taxi zone.',
                       'create_analysis')
            continue

        # add UTC datetime, NY datetime, and UNIX timestamp fields
        utc_time = dt.datetime.strptime(tweet['created_at'],
                                        '%a %b %d %H:%M:%S +0000 %Y')
        tweet['datetimeUTC'] = utc_time
        tweet['datetimeNY'] = pd.to_datetime(utc_time).tz_localize(
            tz='UTC').tz_convert('America/New_York')
        tweet['timestampUNIX'] = utc_time.replace(
            tzinfo=dt.timezone.utc).timestamp()

        # tokenize, convert to lowercase, filter out stop words and
        # punctuation, and stem
        # tweet_tokens = tokenize_tweet(tweet, text_field='text')
        tweet_tokens = [stemmer.stem(token) for token
                        in tknzr.tokenize(tweet['text'])
                        if token.lower() not in stop_list]
        tweet['tokens'] = tweet_tokens
        tokens.update(tweet_tokens)

        # insert processed tweet
        try:
            db[collection].insert_one(tweet)
            insert_num += 1
        except Exception as e:
            fails.append(tweet['id_str'])
            if verbose >= 2:
                output(str(e), 'create_analysis')
    dump(fails, func_name='create_analysis')

    # create indexes
    db[collection].create_index([("coordinates", pymongo.GEOSPHERE)])
    db[collection].create_index([("datetimeUTC", 1), ("location_id", 1)])
    db[collection].create_index([("datetimeUTC", 1), ("borough", 1)])
    db[collection].create_index([("datetimeUTC", 1), ("nyiso_zone", 1)])
    db[collection].create_index([("datetimeNY", 1), ("location_id", 1)])
    db[collection].create_index([("datetimeNY", 1), ("borough", 1)])
    db[collection].create_index([("datetimeNY", 1), ("nyiso_zone", 1)])

    if verbose >= 1:
        output('Finished querying, processing, and inserting tweets from '
               '{tweet_collection} into {collection} collection in {db_name} '
               'database ({insert_num} of {queried_num} queried tweets '
               'inserted).'.format(tweet_collection=tweet_collection,
                                   collection=collection, db_name=db_name,
                                   insert_num=insert_num,
                                   queried_num=insert_num + len(fails)))

    return insert_num, tokens


def create_tweets_keyword(tokens, hashtags, collection='tweets_keyword',
                          analysis_collection='tweets_analysis',
                          db_name='sandy',
                          db_instance='mongodb://localhost:27017/',
                          overwrite=False, verbose=0):
    """Creates a collection of token and/or hashtag-matched tweets. Assumes
    analysis_collection has been processed (using create_analysis). Assumes
    the specified mongodb instance is already running.

    Parameters
    ----------
    tokens : list
        List of tokens to search for.

    hashtags : list
        List of hashtags to search for.

    collection : str
        Name of collection to insert keyword-related tweets into.

    analysis_collection : str
        Name of collection to query tweets from.

    db_name : str
        Name of database to connect to.

    db_instance : str
        Mongodb instance to connect to in URI format.

    overwrite : bool
        Defines whether or not to overwrite existing collection.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    insert_num : int
        Number of tweets inserted into collection.

    Notes
    -----
    Start a mongodb instance by running `$ mongod` from terminal (see
    http://api.mongodb.com/python/current/tutorial.html for more details)
    """

    if verbose >= 1:
        output('Started querying and inserting token and/or hashtag-matched '
               'tweets from {analysis_collection} into {collection} '
               'collection in {db_name} database.'.format(
                analysis_collection=analysis_collection,
                collection=collection, db_name=db_name))

    # connect to db (creates if not exists)
    client = pymongo.MongoClient(db_instance)
    db = client[db_name]

    # overwrite collection if needed
    if overwrite:
        db.drop_collection(collection)
        if verbose >= 1:
            output('Dropped {collection} collection (if exists).'.format(
                collection=collection))

    # query and insert token and/or hashtag-matched tweets
    insert_num = 0
    fails = []
    tweets = query_keyword(tokens=tokens, hashtags=hashtags,
                           collection=analysis_collection, db_name=db_name,
                           db_instance=db_instance, verbose=0)
    for tweet in tweets:
        try:
            db[collection].insert_one(tweet)
            insert_num += 1
        except Exception as e:
            fails.append(tweet['id_str'])
            if verbose >= 2:
                output(str(e), 'create_keyword')
    dump(fails, func_name='create_keyword')

    # create indexes
    db[collection].create_index([("coordinates", pymongo.GEOSPHERE)])
    db[collection].create_index([("datetimeUTC", 1), ("location_id", 1)])
    db[collection].create_index([("datetimeUTC", 1), ("borough", 1)])
    db[collection].create_index([("timestamp", 1)])

    if verbose >= 1:
        output('Finished querying and inserting token and/or hashtag-matched '
               'tweets from {analysis_collection} into {collection} '
               'collection in {db_name} database. ({insert_num} of '
               '{queried_num} queried tweets inserted).'.format(
                analysis_collection=analysis_collection,
                collection=collection, db_name=db_name, insert_num=insert_num,
                queried_num=tweets.count()))

    return insert_num


def create_hydrator_tweetids(path,
                             write_path='data/interim/sandy-tweetids.txt',
                             filter_sandy=False, progressbar=False, verbose=0):
    """Reads the sandy-tweettweet_ids-mdredze.txt file and creates an interim file
    with only tweet tweet_ids (one per line) for input into Hydrator.

    Parameters
    ----------
    path : str
        Path to sandy-tweettweet_ids-mdredze.txt.

    write_path : str
        Full path file name of file to write the tweet tweet_ids to. Existig file
        will be overwritten.

    filter_sandy : boolean, optional
        Determines whether or not to only include tweets that contain the
        word "Sandy".

    progressbar : bool
        If True, displays progress bar. Progress bar does not work when
        called from a notebook in PyCharm.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    num_tweets : int
        Number of tweet tweet_ids written.

    Notes
    -----
    path = "data/raw/release-mdredze.txt"

    Progress bar does not work when called from notebook in PyCharm.

    Example lines in raw file:
    tag:search.twitter.com,2005:260244087901413376	2012-10-22T05:00:00.000Z	False
    tag:search.twitter.com,2005:260244088203403264	2012-10-22T05:00:00.000Z	False
    """

    if verbose >= 1:
        output('Started converting tweet ids from {path} to Hydrator '
               'format.'.format(path=path))

    write_file = open(write_path, 'w')

    # loads and writes tweets line by line
    num_tweets = 0
    num_lines = 0
    with open(path, 'r') as file:
        if progressbar:
            file_iter = tqdm(file)
        else:
            file_iter = file
        for line in file_iter:
            num_lines += 1
            parts = line.strip('\n').split(':')
            # only include tweets containing the word "sandy"
            if filter_sandy:
                sandy_parts = parts[-1]
                sandy = sandy_parts.split('\t')[1]
                if sandy is 'True':
                    date = parts[2]
                    parts = date.split('\t')
                    tweet_id = parts[0]
                    write_file.write(tweet_id + '\n')
                    num_tweets += 1
            # include all tweet tweet_ids
            else:
                date = parts[2]
                parts = date.split('\t')
                tweet_id = parts[0]
                write_file.write(tweet_id + '\n')
                num_tweets += 1
    write_file.close()

    if verbose >= 1:
        output('Finished converting {num_tweets} tweet ids from {path} to '
               'Hydrator format (original file contains {num_lines} '
               'lines).'.format(num_tweets=num_tweets, path=path,
                                num_lines=num_lines))

    return num_tweets


def insert_tweets(path, collection='tweets', db_name='sandy',
                  db_instance='mongodb://localhost:27017/', progressbar=False,
                  overwrite=False, verbose=0):
    """Inserts tweets from a json file into a collection in a mongodb
    database. Assumes the specified mongodb instance is already running.

    Parameters
    ----------
    path : str
        Path to json file containing tweets.  Assumes the json file contains
        one tweet per line.

    collection : str
        Name of collection to insert tweets into.

    db_name : str
        Name of database to connect to.

    db_instance : str
        Mongodb instance to connect to in URI format.

    progressbar : bool
        If True, displays progress bar. Progress bar does not work when
        called from a notebook in PyCharm.

    overwrite : bool
        Defines whether or not to overwrite existing collection.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    insert_num : int
        Number of tweets inserted into collection.

    Notes
    -----
    path = 'data/processed/sandy-tweets-20180314.json'

    Progress bar does not work when called from notebook in PyCharm.

    Dumps failed inserts into a 'data/processed/dump-insert_tweets-[
    datetime].txt' file.

    Start a mongodb instance by running `$ mongod` from terminal (see
    http://api.mongodb.com/python/current/tutorial.html for more details)
    """

    if verbose >= 1:
        output('Started inserting tweets from "{path}" to {collection} '
               'collection in {db_name} database.'.format(
                path=path, collection=collection, db_name=db_name))

    # connect to db (creates if not exists)
    client = pymongo.MongoClient(db_instance)
    db = client[db_name]

    # overwrite collection if needed
    if overwrite:
        db.drop_collection(collection)
        if verbose >= 1:
            output('Dropped {collection} collection (if exists).'.format(
                collection=collection))

    # count lines
    # i = -1
    num_lines = 0
    # with open(path, 'r') as file:
        # for i, line in enumerate(file):
            # pass
    # num_lines = i + 1

    # insert tweets one by one (creates collection if needed)
    insert_num = 0
    fails = []
    with open(path, 'r') as file:
        if progressbar:
            file_iter = tqdm(file, total=num_lines)
        else:
            file_iter = file
        for i, line in enumerate(file_iter):
            tweet = json.loads(line)
            try:
                db[collection].insert_one(tweet)
                insert_num += 1
            except Exception as e:
                fails.append(i)
                if verbose >= 2:
                    output(str(e), 'insert_tweets (line {line_num})'.format(
                        line_num=i))
    dump(fails, func_name='insert_tweets')

    # create indexes
    db[collection].create_index([("coordinates", pymongo.GEOSPHERE)])

    if verbose >= 1:
        output('Finished inserting tweets from "{path}" to {collection} '
               'collection in {db_name} database ({insert_num} tweets inserted'
               'out of {num_lines} lines).'.format(path=path,
                                                   collection=collection,
                                                   db_name=db_name,
                                                   insert_num=insert_num,
                                                   num_lines=num_lines))

    return insert_num


def mongod_to_df(query, collection, db_name='sandy',
                 db_instance='mongodb://localhost:27017/', verbose=0):
    """Query a mongodb database  and return the result as a dataframe.

    Parameters
    ----------
    query : dict
        Dictionary specifying query.

    collection : str
        Name of collection to query from.

    db_name : str
        Name of database to connect to.

    db_instance : str
        Mongodb instance to connect to in URI format.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe
        Query results in a dataframe.

    Notes
    -----
    """

    if verbose >= 1:
        output('Started query.')

    # connect to database and query
    client = pymongo.MongoClient(db_instance)
    db = client[db_name]
    df = pd.DataFrame(list(db[collection].find(query)))

    if verbose >= 1:
        output('Finished query. Returned dataframe with shape {shape}.'.format(
            shape=df.shape()))

    return df


def process_heat_map_daily(df, daterange=None, boroughs=None, verbose=0):
    """ Processes a dataframe for heat map visualization. Proceessing
    includes converting datetime columns, filling missing borough-day
    combinations with nan, and pivoting for use with seaborn.heatmap().

    Parameters
    ----------
    df : dataframe
        Dataframe of daily summary data.

    daterange : list or None
        Specifies date range (inclusive) to include in full dataframe. Specify
        as a list of strings, e.g. ['10/21/2012', '10/27/2012']. If None,
        only includes unique dates found in original dataframe.

    boroughs : list or None
        Specifies boroughs to include in full dataframe. Specify as a list of
        strings, e.g. ['Bronx', 'Brooklyn']. If None, only includes unique
        boroughs found in original dataframe.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df_pivot : dataframe
        Processed dataframe, pivoted for heat map visualization.

    df_proc : dataframe
        Processed dataframe, without pivot.

    Notes
    -----
    """

    # update dtypes and columns
    df['date'] = pd.to_datetime(df['datetimeUTC']).dt.date
    df = df[['date', 'borough', 'count']]

    # build full dataframe (all dates and all boroughs initialized with nans)
    if daterange:
        dates = pd.date_range(start=daterange[0], end=daterange[1]).tolist()
        dates = [pd.Timestamp.to_pydatetime(date).date() for date in dates]
    else:
        dates = df['date'].unique()
    if not boroughs:
        boroughs = sorted(df['borough'].unique())
    df_proc = pd.DataFrame({'date': [], 'borough': [], 'count': []})
    for date in dates:
        for borough in boroughs:
            df_temp = pd.DataFrame({'date': date,
                                    'borough': borough,
                                    'count': [np.nan]})
            df_proc = df_proc.append(df_temp, ignore_index=True)

    # get matching indexes in df_proc of available data in df
    proc_indexes = [df_proc.index[(df_proc['date'] == date) & (
            df_proc['borough'] == borough)].tolist()[0]
                    for date, borough in zip(list(df['date']),
                                             list(df['borough']))]

    # update df_proc with available data in df
    df.index = proc_indexes
    df_proc.loc[proc_indexes, ['count']] = df['count']

    # reformat and rename columns
    df_proc['date'] = df_proc['date'].apply(lambda x: x.strftime('%m-%d'))

    # pivot dataframe for heat map visualization
    df_pivot = df_proc.pivot('borough', 'date', 'count')

    if verbose >= 1:
        output('Processed dataframe for heat map visualization. Original '
               'dataframe shape is {original_shape}. Pivoted '
               'dataframe shape is {pivot_shape}. Processed dataframe '
               'shape is {proc_shape}.'.format(original_shape=df.shape,
                                               pivot_shape=df_pivot.shape,
                                               proc_shape=df_proc.shape))

    return df_pivot, df_proc


def query_groupby(analysis_collection, group_spatial, group_temporal,
                  tweet_count_filter, startdate=None, enddate=None,
                  db_name='sandy',
                  db_instance='mongodb://localhost:27017/', verbose=0):
    """Query and group a collection of tweets for correlation
    analysis. Assumes the collection has been processed for analysis (e.g.
    using create_analysis). This function is slower than query_groupby_hour,
    but is needed because date groupby would require conversion of UTC to
    local tzone within pymongo (or an additional datetime local field in the
    collection).

    Parameters
    ----------
    analysis_collection : str
        Name of analysis (or keyword) collection to query from.

    group_spatial : str
        Name of spatial field to group by: 'zone_id' (nyiso zone),
        'location_id' (taxi zone), or 'borough' (taxi borough).

    group_temporal : str
        Name of temporal field to group by: 'date' or 'hour'.

    tweet_count_filter : int
        Minimum number of tweets in a group.

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

    db_name : str
        Name of database to connect to.

    db_instance : str
        Mongodb instance to connect to in URI format.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df : dataframe

    Notes
    -----
    """

    if verbose >= 1:
        output('Started query.')

    # query tweets
    df = mongod_to_df({}, collection=analysis_collection, db_name=db_name,
                      db_instance=db_instance)

    # check for valid group_spatial arg
    if group_spatial not in df.columns.values:
        raise ValueError('No matching group_spatial argument in collection: '
                         '{arg}.'.format(arg=group_spatial))
    else:
        df = df.rename(columns={group_spatial: 'zone'})

    # convert and filter by datetime
    df['datetimeUTC'] = [datetime.tz_localize(tz='UTC') for datetime in
                         df['datetimeUTC']]
    df['datetime'] = [datetime.tz_convert('America/New_York') for
                      datetime in df['datetimeUTC']]
    df = df[['datetime', 'zone', 'tokens']]
    df = df.set_index('datetime')
    df = df.sort_index()
    df = df.loc[startdate:enddate]
    if verbose >= 2:
        output('[min, max] tweets datetime from {col}: [{min}, '
               '{max}].'.format(col=analysis_collection,
                                min=str(min(df.index.get_level_values(
                                    'datetime'))),
                                max=str(max(df.index.get_level_values(
                                    'datetime')))))

    # group by zone and datetime (date, hour, or none)
    df = df.reset_index()
    if group_temporal:
        if group_temporal == 'date':
            df['datetime'] = df['datetime'].dt.date
        elif group_temporal == 'hour':
            df['datetime'] = df['datetime'].dt.floor('H')
        else:
            raise ValueError('Unknown group_temporal argument: {arg}. See docs '
                             'for valid inputs'.format(arg=group_temporal))
        df_group = df.groupby(['zone', 'datetime']).count()
    else:
        df_group = df.groupby(['zone']).count()
    df_group = df_group.rename(columns={'tokens': 'tweets'})
    df_group = df_group[df_group['tweets'] >= tweet_count_filter]
    if verbose >= 2:
        output('[min, max] number of tweets from {col}: [{min}, {max}].'.format(
            col=analysis_collection, min=str(np.nanmin(df_group['tweets'])),
            max=str(np.nanmax(df_group['tweets']))))

    if verbose >= 1:
        output('Finished query.')

    return df_group


def query_groupby_norm(collection1, collection2, group_spatial, group_temporal,
                       tweet_count_filter, startdate=None, enddate=None,
                       db_name='sandy',
                       db_instance='mongodb://localhost:27017/', verbose=0):
    """Query and return the number of tweets in collection1 normalized by the
    number of tweets in collection2, grouped by spatial and temporal fields.
    Hours are converted to NY tzone. Assumes both collections have been
    processed for analysis (e.g. using create_analysis). This function is slower
    than query_groupby_hour_norm, but is needed because date groupby would
    require conversion of UTC to local tzone within pymongo (or an additional
    datetime local field in the collection).

    Parameters
    ----------
    collection1 : str
        Name of collection to query from, will be normalized by collection2.

    collection2 : str
        Name of collection to query from, will be used to normalize collection1.

    group_spatial : str
        Name of spatial field to group by: 'zone_id' (nyiso zone),
        'location_id' (taxi zone), or 'borough' (taxi borough).

    group_temporal : str
        Name of temporal field to group by: 'date' or 'hour'.

    tweet_count_filter : int
        Minimum number of tweets in a group.

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

    db_name : str
        Name of database to connect to.

    db_instance : str
        Mongodb instance to connect to in URI format.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df_group : dataframe

    Notes
    -----
    """

    if verbose >= 1:
        output('Started query.')

    # query and merge collections
    df1 = query_groupby(collection1, group_spatial, group_temporal,
                        tweet_count_filter, startdate=startdate,
                        enddate=enddate, db_name=db_name,
                        db_instance=db_instance, verbose=2)
    df2 = query_groupby(collection2, group_spatial, group_temporal,
                        tweet_count_filter, startdate=startdate,
                        enddate=enddate, db_name=db_name,
                        db_instance=db_instance, verbose=2)
    df2 = df2.rename(columns={'tweets': 'tweets2'})
    df_group = pd.merge(df2, df1, how='inner', left_index=True,
                        right_index=True)

    # add normalized tweet counts and remove columns
    df_group['tweets-norm'] = df_group['tweets'] / df_group['tweets2']
    df_group = df_group[['tweets', 'tweets-norm']]

    if verbose >= 1:
        output('[min, max] tweets norm: [' +
               str(np.nanmin(df_group['tweets-norm'])) + '], ' +
               str(np.nanmax(df_group['tweets-norm'])) + '].')
        output('Finished query.')

    return df_group


def query_groupby_hour(analysis_collection, group_spatial,
                       tweet_count_filter, startdate=None, enddate=None,
                       db_name='sandy',
                       db_instance='mongodb://localhost:27017/', verbose=0):
    """Query a collection of tweets and group by spatial and temporal fields.
    Hours are converted to NY tzone. Assumes the collection has been
    processed for analysis (e.g. using create_analysis).

    Parameters
    ----------
    analysis_collection : str
        Name of analysis (or keyword) collection to query from.

    group_spatial : str
        Name of spatial field to group by: 'zone_id' (nyiso zone),
        'location_id' (taxi zone), or 'borough' (taxi borough).

    tweet_count_filter : int
        Minimum number of tweets in a group.

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

    db_name : str
        Name of database to connect to.

    db_instance : str
        Mongodb instance to connect to in URI format.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df_group : dataframe

    Notes
    -----
    """

    if verbose >= 1:
        output('Started query.')

    # connect to db (creates if not exists)
    client = pymongo.MongoClient(db_instance)
    db = client[db_name]

    # check for valid groupby args
    df_temp = db[analysis_collection].find_one()
    if group_spatial not in df_temp.keys():
        raise ValueError('No matching group_spatial argument in collection: '
                         '{arg}.'.format(arg=group_spatial))

    # query grouped tweets
    pipeline = [
        {
            "$group": {
                "_id": {
                    "year": {"$year": "$datetimeUTC"},
                    "month": {"$month": "$datetimeUTC"},
                    "day": {"$dayOfMonth": "$datetimeUTC"},
                    "hour": {"$hour": "$datetimeUTC"},
                    group_spatial: "${spatial}".format(spatial=group_spatial)
                },
                "datetimeUTC": {"$min": {"$dateFromParts": {
                    "year": {"$year": "$datetimeUTC"},
                    "month": {"$month": "$datetimeUTC"},
                    "day": {"$dayOfMonth": "$datetimeUTC"},
                    "hour": {"$hour": "$datetimeUTC"}
                }}},
                group_spatial: {"$first": "${spatial}".format(spatial=group_spatial)},
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"datetimeUTC": 1, "_id.zone": 1}}
    ]
    # if group_temporal == 'date':
    #     pipeline[0]['$group']['_id'].pop('hour', None)
    #     pipeline[0]['$group']['datetimeUTC']['$min']['$dateFromParts'].pop(
    #         'hour', None)
    groups = list(db[analysis_collection].aggregate(pipeline))

    # convert to dataframe and drop missing spatial groups
    df_group = pd.DataFrame(groups)
    df_group = df_group.dropna(subset=[group_spatial])

    # make timezone aware
    df_group['datetimeUTC'] = [datetime.tz_localize(tz='UTC') for datetime
                               in df_group['datetimeUTC']]
    df_group['datetime'] = [datetime.tz_convert('America/New_York') for
                            datetime in df_group['datetimeUTC']]
    df_group = df_group[['datetime', group_spatial, 'count']]

    # filter by datetime
    df_group = df_group.set_index('datetime')
    df_group = df_group.sort_index()
    df_group = df_group.loc[startdate:enddate]
    if verbose >= 2:
        output('[min, max] tweets datetime from {col}: [{min}, '
               '{max}].'.format(col=analysis_collection,
                                min=str(min(df_group.index.get_level_values(
                                    'datetime'))),
                                max=str(max(df_group.index.get_level_values(
                                    'datetime')))))

    # filter by tweet count
    df_group = df_group.reset_index()
    df_group = df_group.set_index([group_spatial, 'datetime'])
    df_group = df_group.sort_index()
    df_group = df_group.rename(columns={'count': 'tweets'})
    df_group = df_group[df_group['tweets'] >= tweet_count_filter]
    if verbose >= 2:
        output('[min, max] number of tweets from {col}: [{min}, {max}].'.format(
            col=analysis_collection, min=str(np.nanmin(df_group['tweets'])),
            max=str(np.nanmax(df_group['tweets']))))

    if verbose >= 1:
        output('Finished query.')

    return df_group


def query_groupby_hour_norm(collection1, collection2, group_spatial,
                            tweet_count_filter, startdate=None, enddate=None,
                            db_name='sandy',
                            db_instance='mongodb://localhost:27017/',
                            verbose=0):
    """Query and return the number of tweets in collection1 normalized by the
    number of tweets in collection2, grouped by spatial and temporal fields.
    Hours are converted to NY tzone. Assumes both collections have been
    processed for analysis (e.g. using create_analysis).

    Parameters
    ----------
    collection1 : str
        Name of collection to query from, will be normalized by collection2.

    collection2 : str
        Name of collection to query from, will be used to normalize collection1.

    group_spatial : str
        Name of spatial field to group by: 'zone_id' (nyiso zone),
        'location_id' (taxi zone), or 'borough' (taxi borough).

    tweet_count_filter : int
        Minimum number of tweets in a group.

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

    db_name : str
        Name of database to connect to.

    db_instance : str
        Mongodb instance to connect to in URI format.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    df_group : dataframe

    Notes
    -----
    """

    if verbose >= 1:
        output('Started query.')

    # query and merge collections
    df1 = query_groupby_hour(collection1, group_spatial, tweet_count_filter,
                             startdate=startdate, enddate=enddate,
                             db_name=db_name, db_instance=db_instance,
                             verbose=2)
    df2 = query_groupby_hour(collection2, group_spatial, tweet_count_filter,
                             startdate=startdate, enddate=enddate,
                             db_name=db_name, db_instance=db_instance,
                             verbose=2)
    df2 = df2.rename(columns={'tweets': 'tweets2'})
    df_group = pd.merge(df2, df1, how='inner', left_index=True,
                        right_index=True)

    # add normalized tweet counts and remove columns
    df_group['tweets-norm'] = df_group['tweets'] / df_group['tweets2']
    df_group = df_group[['tweets', 'tweets-norm']]

    if verbose >= 1:
        output('[min, max] tweets norm: [' +
               str(np.nanmin(df_group['tweets-norm'])) + '], ' +
               str(np.nanmax(df_group['tweets-norm'])) + '].')
        output('Finished query.')

    return df_group


def query_keyword(tokens=None, hashtags=None, collection='tweets_analysis',
                  db_name='sandy', db_instance='mongodb://localhost:27017/',
                  verbose=0):
    """Query a mongodb database based on keywords (i.e. tokens) and/or hashtags.

    Parameters
    ----------
    tokens : list
        List of tokens to search for.

    hashtags : list
        List of hashtags to search for.

    collection : str
        Name of collection to query from.

    db_name : str
        Name of database to connect to.

    db_instance : str
        Mongodb instance to connect to in URI format.

    verbose : int
        Defines verbosity for output statements.

    Returns
    -------
    docs : cursor
        Cursor object of matching documents.

    Notes
    -----
    """

    if verbose >= 1:
        output('Started query.')

    # connect to database and query
    client = pymongo.MongoClient(db_instance)
    db = client[db_name]
    if tokens and hashtags:
        query_dict = {
            "$or": [
                {"tokens": {"$in": tokens}},
                {"entities.hashtags.text": {"$in": hashtags}}
            ]
        }
    elif tokens:
        query_dict = {"tokens": {"$in": tokens}}
    elif hashtags:
        query_dict = {"entities.hashtags.text": {"$in": hashtags}}
    else:
        output('Error: Must specify at least one of tokens or hashtags '
               'arguments.', 'query_keyword')
        return None
    tweets = db[collection].find(query_dict)

    if verbose >= 1:
        output('Finished query. Returned {num_tweets} tweets.'.format(
            num_tweets=tweets.count()))

    return tweets


def tokenize_tweet(tweet_text):
    """Tokenizes a tweet text string.

    Parameters
    ----------
    tweet_text : str
        Text string to tokenize. Assumes this is from a tweet (i.e. uses the
        TweetTokenizer).

    Returns
    -------
    tokens : list
        List of tokens in tweet.

    Notes
    -----
    Requires download of nltk data (tested with popular package). See
    https://www.nltk.org/data.html for download details.
    """

    tknzr = nltk.tokenize.TweetTokenizer(strip_handles=True, reduce_len=True)
    stop_list = nltk.corpus.stopwords.words("english") + list(
        string.punctuation)
    stemmer = nltk.stem.PorterStemmer()
    tweet_tokens = [stemmer.stem(token) for token in tknzr.tokenize(tweet_text)
                    if token.lower() not in stop_list]

    return tweet_tokens

# def create_summary(analysis_collection, byborough=True, byday=True,
#                    title=None, db_name='sandy',
#                    db_instance='mongodb://localhost:27017/', overwrite=False,
#                    verbose=0):
#     """Creates a collection and dataframe of summary statistics for tweets
#     from the analysis_collection collection grouped by borough/zone and
#     day/hour. Assumes analysis_collection has been processed (using
#     create_analysis). Assumes the specified mongodb instance is already running.
#
#     TODO - currently replaced by query_correlation_groupby (see issue #98).
#
#     Parameters
#     ----------
#     analysis_collection : str
#         Name of collection to query tweets from.
#
#     byborough : bool
#         If True, groups by borough. If False, groups by zone (i.e. location_id).
#
#     byday : bool
#         If True, groups by year-month-day. If False, groups by
#         year-month-day-hour.
#
#     title : str or None
#         Defines the suffix of the borough/zone_day/hour_summary_[title]
#         collection to be created. E.g. borough_hour_summary_traffic.
#
#     db_name : str
#         Name of database to connect to.
#
#     db_instance : str
#         Mongodb instance to connect to in URI format.
#
#     overwrite : bool
#         Defines whether or not to overwrite existing collection.
#
#     verbose : int
#         Defines verbosity for output statements.
#
#     Returns
#     -------
#     borough_days : list
#         List of
#
#     Notes
#     -----
#     Start a mongodb instance by running `$ mongod` from terminal (see
#     http://api.mongodb.com/python/current/tutorial.html for more details)
#     """
#
#     if byborough and byday:
#         collection = 'borough_day_{title}'.format(title=title)
#     elif byborough and not byday:
#         collection = 'borough_hour_{title}'.format(title=title)
#     elif not byborough and byday:
#         collection = 'zone_day_{title}'.format(title=title)
#     elif not byborough and not byday:
#         collection = 'zone_hour_{title}'.format(title=title)
#     else:
#         output('Error: Unexpected arguments for byborough and/or byday.',
#                'create_summary')
#         return None
#
#     if verbose >= 1:
#         output('Started summarizing tweets from {analysis_collection} into '
#                '{collection} collection in {db_name} database.'.format(
#                 analysis_collection=analysis_collection,
#                 collection=collection, db_name=db_name))
#
#     # connect to db (creates if not exists)
#     client = pymongo.MongoClient(db_instance)
#     db = client[db_name]
#
#     # overwrite collection if needed
#     if overwrite:
#         db.drop_collection(collection)
#         if verbose >= 1:
#             output('Dropped {collection} collection (if exists).'.format(
#                 collection=collection))
#
#     # query grouped tweets
#     if byborough and byday:
#         pipeline = [
#             {
#                 "$group": {
#                     "_id": {
#                         "year": {"$year": "$datetimeUTC"},
#                         "month": {"$month": "$datetimeUTC"},
#                         "day": {"$dayOfMonth": "$datetimeUTC"},
#                         "borough": "$borough"
#                     },
#                     "datetimeUTC": {"$min": {"$dateFromParts": {
#                         "year": {"$year": "$datetimeUTC"},
#                         "month": {"$month": "$datetimeUTC"},
#                         "day": {"$dayOfMonth": "$datetimeUTC"}
#                     }}},
#                     "borough": {"$first": "$borough"},
#                     "count": {"$sum": 1}
#                 }
#             },
#             {"$sort": {"datetimeUTC": 1, "_id.borough": 1}}
#         ]
#     elif byborough and not byday:
#         pipeline = [
#             {
#                 "$group": {
#                     "_id": {
#                         "year": {"$year": "$datetimeUTC"},
#                         "month": {"$month": "$datetimeUTC"},
#                         "day": {"$dayOfMonth": "$datetimeUTC"},
#                         "hour": {"$hour": "$datetimeUTC"},
#                         "borough": "$borough"
#                     },
#                     "datetimeUTC": {"$min": {"$dateFromParts": {
#                         "year": {"$year": "$datetimeUTC"},
#                         "month": {"$month": "$datetimeUTC"},
#                         "day": {"$dayOfMonth": "$datetimeUTC"},
#                         "hour": {"$hour": "$datetimeUTC"}
#                     }}},
#                     "borough": {"$first": "$borough"},
#                     "count": {"$sum": 1}
#                 }
#             },
#             {"$sort": {"datetimeUTC": 1, "_id.borough": 1}}
#         ]
#     elif not byborough and byday:
#         pipeline = [
#             {
#                 "$group": {
#                     "_id": {
#                         "year": {"$year": "$datetimeUTC"},
#                         "month": {"$month": "$datetimeUTC"},
#                         "day": {"$dayOfMonth": "$datetimeUTC"},
#                         "zone": "$location_id"
#                     },
#                     "datetimeUTC": {"$min": {"$dateFromParts": {
#                         "year": {"$year": "$datetimeUTC"},
#                         "month": {"$month": "$datetimeUTC"},
#                         "day": {"$dayOfMonth": "$datetimeUTC"}
#                     }}},
#                     "zone": {"$first": "$location_id"},
#                     "count": {"$sum": 1}
#                 }
#             },
#             {"$sort": {"datetimeUTC": 1, "_id.zone": 1}}
#         ]
#     elif not byborough and not byday:
#         pipeline = [
#             {
#                 "$group": {
#                     "_id": {
#                         "year": {"$year": "$datetimeUTC"},
#                         "month": {"$month": "$datetimeUTC"},
#                         "day": {"$dayOfMonth": "$datetimeUTC"},
#                         "hour": {"$hour": "$datetimeUTC"},
#                         "zone": "$location_id"
#                     },
#                     "datetimeUTC": {"$min": {"$dateFromParts": {
#                         "year": {"$year": "$datetimeUTC"},
#                         "month": {"$month": "$datetimeUTC"},
#                         "day": {"$dayOfMonth": "$datetimeUTC"},
#                         "hour": {"$hour": "$datetimeUTC"}
#                     }}},
#                     "zone": {"$first": "$location_id"},
#                     "count": {"$sum": 1}
#                 }
#             },
#             {"$sort": {"datetimeUTC": 1, "_id.zone": 1}}
#         ]
#     else:
#         pipeline = None
#     groups = list(db[analysis_collection].aggregate(pipeline))
#
#     # insert summary documents
#     insert_num = 0
#     fails = []
#     for i, group in enumerate(groups):
#         try:
#             db[collection].insert_one(group)
#             insert_num += 1
#         except Exception as e:
#             fails.append(i)
#             if verbose >= 2:
#                 output(str(e), 'create_summary')
#     dump(fails, func_name='create_summary')
#
#     # convert to dataframe and make timezone aware
#     df = pd.DataFrame(groups)
#     df['datetimeUTC'] = [datetime.tz_localize(tz='UTC') for datetime
#                          in df['datetimeUTC']]
#
#     if verbose >= 1:
#         output('Finished summarizing tweets from {analysis_collection} into '
#                '{collection} collection in {db_name} database ({insert_num} '
#                'of {groups} queried summaries inserted).'.format(
#                 analysis_collection=analysis_collection,
#                 collection=collection, db_name=db_name, insert_num=insert_num,
#                 groups=len(groups)))
#
#     return df


# def create_spatial_summary(analysis_collection, byborough=True, title=None,
#                            db_name='sandy',
#                            db_instance='mongodb://localhost:27017/',
#                            overwrite=False, verbose=0):
#     """Creates a collection and dataframe of summary statistics for tweets
#     from the analysis_collection collection grouped by borough/zone across
#     all times within the collection. Assumes analysis_collection has been
#     processed (using create_analysis). Assumes the specified mongodb instance is
#     already running.
#
#     TODO - currently replaced by query_correlation_groupby (see issue #98).
#
#     Parameters
#     ----------
#     analysis_collection : str
#         Name of collection to query tweets from.
#
#     byborough : bool
#         If True, groups by borough. If False, groups by zone (i.e. location_id).
#
#     title : str or None
#         Defines the suffix of the borough/zone_summary_[title]
#         collection to be created. E.g. borough_summary_traffic.
#
#     db_name : str
#         Name of database to connect to.
#
#     db_instance : str
#         Mongodb instance to connect to in URI format.
#
#     overwrite : bool
#         Defines whether or not to overwrite existing collection.
#
#     verbose : int
#         Defines verbosity for output statements.
#
#     Returns
#     -------
#     borough_days : list
#         List of
#
#     Notes
#     -----
#     Start a mongodb instance by running `$ mongod` from terminal (see
#     http://api.mongodb.com/python/current/tutorial.html for more details)
#     """
#
#     if byborough:
#         collection = 'borough_{title}'.format(title=title)
#     else:
#         collection = 'zone_{title}'.format(title=title)
#
#     if verbose >= 1:
#         output('Started summarizing tweets from {analysis_collection} into '
#                '{collection} collection in {db_name} database.'.format(
#                 analysis_collection=analysis_collection,
#                 collection=collection, db_name=db_name))
#
#     # connect to db (creates if not exists)
#     client = pymongo.MongoClient(db_instance)
#     db = client[db_name]
#
#     # overwrite collection if needed
#     if overwrite:
#         db.drop_collection(collection)
#         if verbose >= 1:
#             output('Dropped {collection} collection (if exists).'.format(
#                 collection=collection))
#
#     # query grouped tweets
#     if byborough:
#         pipeline = [
#             {
#                 "$group": {
#                     "_id": {"borough": "$borough"},
#                     "borough": {"$first": "$borough"},
#                     "count": {"$sum": 1}
#                 }
#             },
#             {"$sort": {"_id.borough": 1}}
#         ]
#     else:
#         pipeline = [
#             {
#                 "$group": {
#                     "_id": {"zone": "$location_id"},
#                     "zone": {"$first": "$location_id"},
#                     "count": {"$sum": 1}
#                 }
#             },
#             {"$sort": {"_id.zone": 1}}
#         ]
#     groups = list(db[analysis_collection].aggregate(pipeline))
#
#     # insert summary documents
#     insert_num = 0
#     fails = []
#     for i, group in enumerate(groups):
#         try:
#             db[collection].insert_one(group)
#             insert_num += 1
#         except Exception as e:
#             fails.append(i)
#             if verbose >= 2:
#                 output(str(e), 'create_spatial_summary')
#     dump(fails, func_name='create_spatial_summary')
#
#     # convert to dataframe
#     df = pd.DataFrame(groups)
#
#     if verbose >= 1:
#         output('Finished summarizing tweets from {analysis_collection} into '
#                '{collection} collection in {db_name} database ({insert_num} '
#                'of {groups} queried summaries inserted).'.format(
#                 analysis_collection=analysis_collection,
#                 collection=collection, db_name=db_name, insert_num=insert_num,
#                 groups=len(groups)))
#
#     return df
