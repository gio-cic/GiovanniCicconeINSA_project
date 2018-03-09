# Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from elasticsearch import Elasticsearch, TransportError
import sys
import time
import urllib.request
import os
import configparser

#read config file

config = configparser.ConfigParser()
config.sections()
config.read('config.ini')

index_conf = config['DEFAULT']['index']
doc_type_conf = config['DEFAULT']['doc_type']
host_conf = config['DEFAULT']['host']
port_conf = config['DEFAULT']['port']
image_directory_conf = config['DEFAULT']['image_directory']

# Variables that contains the user credentials to access Twitter API
access_token = "622059145-UNN7hFb8bmEPNB4sp8AbDwrrlInMSQ7DU1EuMNtk"
access_token_secret = "1IaarCwEUCVWwBk1lhHsYp3QoRnOokt3xo6SECLbz9Ciy"
consumer_key = "qOtasggxIgJOLZYHblQRlB1gu"
consumer_secret = "d6f2QHHg13ffbHS5YXhbJhXjPzusGLHWGXJtmnCDwjlm79ddsV"


# Listener that loads the tweets in an elasticsearch index
class StdOutListener(StreamListener):
    i = 1

    def __init__(self, time_limit=15):
        self.start_time = time.time()
        self.limit = time_limit
        super(StdOutListener, self).__init__()

    def on_error(self, status):
        print("there is an error" + status)

    def on_status(self, status):
        if (time.time() - self.start_time) < self.limit:
            try:
                time.sleep(1)
                # print(status.author.screen_name, status.created_at)
                json_data = status._json
                es.create(index="my_index", doc_type="my_doc_type", id=StdOutListener.i, body=json_data)
                mapping = es.indices.get_mapping(index="my_index", doc_type="my_doc_type")
                print(mapping)
                print(json_data)
                print(StdOutListener.i)
                StdOutListener.i = StdOutListener.i + 1
            except:
                print("Unexpected error:", sys.exc_info()[1])
                pass
            return True
        else:
            return False

if __name__ == '__main__':

    # prepare W directory
    if os.path.isdir(image_directory_conf) is False:
        try:
            original_umask = os.umask(0)
            os.makedirs(image_directory_conf, 777)
        finally:
            os.umask(original_umask)

    es = Elasticsearch([{'host': host_conf, 'port': int(port_conf)}])
    mapping = '''
    {"mappings": {"my_doc_type": {"properties": {"created_at": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "display_text_range": {"type": "long"}, "entities": {"properties": {"hashtags": {"properties": {"indices": {"type": "long"}, "text": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}, "media": {"properties": {"display_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "expanded_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "indices": {"type": "long"}, "media_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "media_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "sizes": {"properties": {"large": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "medium": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "small": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "thumb": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}}}, "type": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}, "urls": {"properties": {"display_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "expanded_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "indices": {"type": "long"}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}}}, "extended_entities": {"properties": {"media": {"properties": {"display_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "expanded_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "indices": {"type": "long"}, "media_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "media_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "sizes": {"properties": {"large": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "medium": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "small": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "thumb": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}}}, "type": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}}}, "favorite_count": {"type": "long"}, "favorited": {"type": "boolean"}, "filter_level": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "is_quote_status": {"type": "boolean"}, "lang": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "possibly_sensitive": {"type": "boolean"}, "quote_count": {"type": "long"}, "reply_count": {"type": "long"}, "retweet_count": {"type": "long"}, "retweeted": {"type": "boolean"}, "source": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "text": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "timestamp_ms": {"type": "date", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "truncated": {"type": "boolean"}, "user": {"properties": {"contributors_enabled": {"type": "boolean"}, "created_at": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "default_profile": {"type": "boolean"}, "default_profile_image": {"type": "boolean"}, "description": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "favourites_count": {"type": "long"}, "followers_count": {"type": "long"}, "friends_count": {"type": "long"}, "geo_enabled": {"type": "boolean"}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "is_translator": {"type": "boolean"}, "lang": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "listed_count": {"type": "long"}, "location": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_image_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_image_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_tile": {"type": "boolean"}, "profile_banner_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_image_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_image_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_link_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_sidebar_border_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_sidebar_fill_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_text_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_use_background_image": {"type": "boolean"}, "protected": {"type": "boolean"}, "screen_name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "statuses_count": {"type": "long"}, "time_zone": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "translator_type": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "utc_offset": {"type": "long"}, "verified": {"type": "boolean"}}}}}}}
    '''
    #{"mappings": {"my_doc_type": {"properties": {"created_at": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "display_text_range": {"type": "long"}, "entities": {"properties": {"hashtags": {"properties": {"indices": {"type": "long"}, "text": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}, "media": {"properties": {"display_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "expanded_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "indices": {"type": "long"}, "media_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "media_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "sizes": {"properties": {"large": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "medium": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "small": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "thumb": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}}}, "type": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}, "urls": {"properties": {"display_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "expanded_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "indices": {"type": "long"}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}}}, "extended_entities": {"properties": {"media": {"properties": {"display_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "expanded_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "indices": {"type": "long"}, "media_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "media_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "sizes": {"properties": {"large": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "medium": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "small": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "thumb": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}}}, "type": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}}}, "favorite_count": {"type": "long"}, "favorited": {"type": "boolean"}, "filter_level": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "is_quote_status": {"type": "boolean"}, "lang": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "possibly_sensitive": {"type": "boolean"}, "quote_count": {"type": "long"}, "reply_count": {"type": "long"}, "retweet_count": {"type": "long"}, "retweeted": {"type": "boolean"}, "source": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "text": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "timestamp_ms": {"type": "date", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "truncated": {"type": "boolean"}, "user": {"properties": {"contributors_enabled": {"type": "boolean"}, "created_at": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "default_profile": {"type": "boolean"}, "default_profile_image": {"type": "boolean"}, "description": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "favourites_count": {"type": "long"}, "followers_count": {"type": "long"}, "friends_count": {"type": "long"}, "geo_enabled": {"type": "boolean"}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "is_translator": {"type": "boolean"}, "lang": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "listed_count": {"type": "long"}, "location": {"type": "geo_point", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_image_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_image_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_tile": {"type": "boolean"}, "profile_banner_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_image_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_image_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_link_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_sidebar_border_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_sidebar_fill_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_text_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_use_background_image": {"type": "boolean"}, "protected": {"type": "boolean"}, "screen_name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "statuses_count": {"type": "long"}, "time_zone": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "translator_type": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "utc_offset": {"type": "long"}, "verified": {"type": "boolean"}}}}}}}
    es.indices.create(index='my_index', body=mapping)
    print(es.indices.get_mapping(index='my_index'))
    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    while True:
        numKeywords = int(input("Insert the number of keywords you want: "))
        i = 0
        keywords = []
        while i < numKeywords:
            keyword = input("Insert keyword: ")
            keywords.append(keyword)
            i = i + 1
        print(keywords)
        print("before stream")
        stream = Stream(auth=auth, listener=StdOutListener(time_limit=30))
        stream.filter(track=keywords)
        print("after stream")
        stream = None
