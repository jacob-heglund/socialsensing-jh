# -*- coding: utf-8 -*-
"""
Functions for testing twitterinfrastructure.twitter_sandy module.


"""

import pymongo
from twitterinfrastructure import twitter_sandy as ts
import unittest


class TestTwitter(unittest.TestCase):
    hydrator_path = 'tests/twitter_sandy/raw/release-mdredze-short_test.txt'
    write_path = 'tests/twitter_sandy/interim/sandy-tweetids-short_test.txt'
    insert_path = 'tests/twitter_sandy/processed/sandy-tweets-short-20180523_test.json'
    collection = 'tweets_test'
    db_name = 'test'
    db_instance = 'mongodb://localhost:27017/'

    def test_create_hydrator_tweetids(self):
        num_tweets = ts.create_hydrator_tweetids(self.hydrator_path, write_path=self.write_path, filter_sandy=False)

        with open(self.write_path) as file:
            lines_test = file.read().split('\n')

        lines_test = [int(line) for line in lines_test if line]

        lines_true = [260244087901413376, 260244088203403264, 260244088161439744,
                      260244088819945472, 260244089080004609, 260244089985957888,
                      260244092527706112, 260244093119102977, 260244093257515008,
                      260244094939439105]

        assert num_tweets == 10 and lines_test == lines_true

    def test_insert_tweets(self):

        insert_num = ts.insert_tweets(self.insert_path, collection=self.collection,
                                      db_name=self.db_name, db_instance=self.db_instance,
                                      overwrite=True, verbose=2)

        client = pymongo.MongoClient(self.db_instance)
        db = client[self.db_name]
        tweets = db[self.collection].find()

        assert insert_num == 8 and tweets.count() == 8 and \
               tweets[0]['coordinates']['coordinates'][0] == -76.8206691 and \
               tweets[0]['id_str'] == '260244088161439744'



# def test_query_keyword():
#
#     collection = 'tweets_analysis_test'
#     db_name = 'test'
#     db_instance = 'mongodb://localhost:27017/'
#
#     tokens = ['gust', 'humid', 'idk']
#     tweets = ts.query_keyword(tokens=tokens,
#                               collection=collection, db_name=db_name,
#                               db_instance=db_instance, verbose=1)
#
#     assert tweets.count() == 2 and \
#         tweets['id_str'][0] =='260244089985957888' and \
#         tweets['id_str'][1] == '260244093119102977'
