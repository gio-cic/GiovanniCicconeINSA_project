
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import time
import sys

import configparser
config = configparser.ConfigParser()
config.sections()
config.read('tokens.ini')
access_token = config['DEFAULT']['access_token']
access_token_secret = config['DEFAULT']['access_token_secret']
consumer_key = config['DEFAULT']['consumer_key']
consumer_secret = config['DEFAULT']['consumer_secret']

class StdOutListener(StreamListener):

    def __init__(self, elasticsearch, time_limit=15):
        self.i =0
        self.start_time = time.time()
        self.limit = time_limit
        self.elasticsearch = elasticsearch
        super(StdOutListener, self).__init__()

    def on_data(self, data):
        if (time.time() - self.start_time) < self.limit:
            self.elasticsearch.save_in_elasticsearch(data, self.i)
            self.i += 1
            return True
        else:
            return False

    def on_error(self, status):
        print("there is an error"+str(status))


class collect_tweets:
    def __init__(self, elasticsearch, minutes_collecting = 1, keywords = ['trump', 'miami']):
        self.elasticsearch = elasticsearch
        self.minutes_collecting = minutes_collecting
        self.keywords = keywords
        # This handles Twitter authetification and the connection to Twitter Streaming API
        l = StdOutListener(self.elasticsearch, time_limit=60 * int(minutes_collecting))
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        print("keywords are " + str(keywords))
        print("collecting... wait...")
        while True:
            try:
                stream = Stream(auth=auth, listener=l)
                stream.filter(track=keywords)
            except:
                print("Unexpected error in save_in_elasticsearch", sys.exc_info())
                pass
