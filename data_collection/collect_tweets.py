
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import time
import sys

import configparser
config = configparser.ConfigParser()
config.sections()
config.read('C:/Users/giovanni/PycharmProjects/GiovanniCicconeINSA_project/tokens.ini')
access_token = config['DEFAULT']['access_token']
access_token_secret = config['DEFAULT']['access_token_secret']
consumer_key = config['DEFAULT']['consumer_key']
consumer_secret = config['DEFAULT']['consumer_secret']

class StdOutListener(StreamListener):

    def __init__(self, elasticsearch_management, time_limit=15):
        self.i =0
        self.start_time = time.time()
        self.limit = time_limit
        self.elasticsearch_management = elasticsearch_management
        super(StdOutListener, self).__init__()

    def on_data(self, data):
        if (time.time() - self.start_time) < self.limit:
            save_tweet_in_elasticsearch(data, self)
            self.i += 1
            return True
        else:
            return False

    def on_error(self, status_code):
        print("there is an error of elasticsearch " + str(status_code))
        if status_code == 420:
            # returning False in on_data disconnects the stream
            time.sleep(60*10)
            return False

def save_tweet_in_elasticsearch(json_data, obj):
    try:
        obj.elasticsearch_management.elasticsearch.create(index=obj.elasticsearch_management.index, doc_type=obj.elasticsearch_management.doctype, id=int(obj.i), body=json_data, ignore=400)
        #print(str(obj.i)+" "+json_data)
        obj.i = obj.i + 1

    except:
        print("Unexpected error in save_in_elasticsearch", sys.exc_info()[1])
        obj.i = obj.i + 1
        pass


class collect_tweets:
    def __init__(self, elasticsearch_management, minutes_collecting = 1, keywords = ['trump', 'miami']):
        self.elasticsearch_management = elasticsearch_management
        self.minutes_collecting = minutes_collecting
        self.keywords = keywords
        # This handles Twitter authetification and the connection to Twitter Streaming API
        l = StdOutListener(self.elasticsearch_management, time_limit=60 * int(minutes_collecting))
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        stream = Stream(auth=auth, listener=l)
        print("keywords are " + str(keywords))
        print('collecting_tweets.log contains log info about the collecting process')
        f = open('collecting_tweets.txt', "w")
        f.write("log for problems in collecting tweets\n")
        f.close()
        while True:
            try:
                print("collecting... wait...")
                stream = Stream(auth=auth, listener=l)
                stream.filter(track=keywords)
                stream = None
                time.sleep(60)
            except:
                print("Unexpected error in collect_tweets ", sys.exc_info())
                f = open('collecting_tweets.txt', "a")
                f.write(str(str(sys.exc_info())+"\n"+"****************************************"+"\n"))
                f.close()


