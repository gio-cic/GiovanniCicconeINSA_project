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
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from collections import Counter
from prettytable import PrettyTable

#read config file
config = configparser.ConfigParser()
config.sections()
config.read('config.ini')

INDEX_CONF = config['DEFAULT']['index']
DOC_TYPE_CONF = config['DEFAULT']['doc_type']
HOST_CONF = config['DEFAULT']['host']
PORT_CONF = config['DEFAULT']['port']
IMAGE_DIRECTORY_CONF = config['DEFAULT']['image_directory']
TIME_SLOT_CONF = config['DEFAULT']['time_slot']

# Variables that contains the user credentials to access Twitter API
access_token = "622059145-UNN7hFb8bmEPNB4sp8AbDwrrlInMSQ7DU1EuMNtk"
access_token_secret = "1IaarCwEUCVWwBk1lhHsYp3QoRnOokt3xo6SECLbz9Ciy"
consumer_key = "qOtasggxIgJOLZYHblQRlB1gu"
consumer_secret = "d6f2QHHg13ffbHS5YXhbJhXjPzusGLHWGXJtmnCDwjlm79ddsV"

stopWords = set(stopwords.words('english'))

# Listener that loads the tweets in an elasticsearch index
class StdOutListener(StreamListener):
    start_index = 1
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
                # print(status.author.screen_name, status.created_at)
                json_data = status._json

                #data = json_data['text']
                #words = word_tokenize(data)
                #wordsFiltered = []
                #for w in words:
                #   if w not in stopWords:
                #        wordsFiltered.append(w)
                #json_data['text'] = wordsFiltered

                es.create(index=INDEX_CONF, doc_type=DOC_TYPE_CONF, id=StdOutListener.i, body=json_data, ignore=400)
                #mapping = es.indices.get_mapping(index=INDEX_CONF, doc_type=DOC_TYPE_CONF)
                #print(mapping)

                #frequency-based analysis of tweet text



                #print(str(StdOutListener.i)+" "+str(json_data))
                StdOutListener.i = StdOutListener.i + 1

                tweet_id = json_data['id']
                try:
                    array_media = json_data['extended_entities']['media']
                    counter_media = 0
                    for media in array_media:
                        if media['type'] == 'photo':
                            #print("image saved")
                            url = media['media_url']
                            #urllib.request.urlretrieve(url, IMAGE_DIRECTORY_CONF + "/" + str(tweet_id) + "_" + str(counter_media))
                            counter_media = counter_media + 1
                except:
                    pass
            except:
                print("Unexpected error:", sys.exc_info()[1])
                pass
            return True
        else:
            return False

def newKeywords(index, doc_type, start_index, size):
    query_result = es.search(index=index, doc_type=DOC_TYPE_CONF, from_=int(start_index),size=int(size))
    print(query_result)
    tweets = []
    i=0
    for i in range(i,int(size)):

        try:
            text = query_result['hits']['hits'][i]['_source']['retweeted_status']['extended_tweet']['full_text']
            tweets.append(text)
        except:
            try:
                text = query_result['hits']['hits'][i]['_source']['text']
                tweets.append(text)
            except:
                pass
    print(tweets)

    stopWords = set(stopwords.words('english'))
    counter = Counter()
    text_tokenized = None
    for text in tweets:
        text_tokenized_original = word_tokenize(text)
        text_tokenized = []
        for w in text_tokenized_original:
            if w not in stopWords:
                if w not in [',', '.', ';', ':', "'", "@", "#", "https", "RT", "\"", "''", "I", "OF", "The", "You"]:
                    if w.isalpha():
                        text_tokenized.append(w)
        for item in text_tokenized:
            counter[item] = counter[item] + 1
    print(counter.most_common()[:3])  # top 10
    pt = PrettyTable(field_names=['Word', 'Count'])
    [pt.add_row(kv) for kv in counter.most_common()[:10]]
    pt.align['Word'], pt.align['Count'] = 'l', 'r'  # Set column alignment
    print(pt)
    time.sleep(4)
    return list(map((lambda x: x[0]), counter.most_common()[:3]))

if __name__ == '__main__':

    # prepare W directory
    if os.path.isdir(IMAGE_DIRECTORY_CONF) is False:
        try:
            original_umask = os.umask(0)
            os.makedirs(IMAGE_DIRECTORY_CONF, 777)
        finally:
            os.umask(original_umask)

    es = Elasticsearch([{'host': HOST_CONF, 'port': int(PORT_CONF)}])
    mapping = '''
        {"mappings": {"my_doc_type": {"properties": {"created_at": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "display_text_range": {"type": "long"}, "entities": {"properties": {"hashtags": {"properties": {"indices": {"type": "long"}, "text": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}, "media": {"properties": {"display_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "expanded_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "indices": {"type": "long"}, "media_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "media_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "sizes": {"properties": {"large": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "medium": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "small": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "thumb": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}}}, "type": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}, "urls": {"properties": {"display_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "expanded_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "indices": {"type": "long"}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}}}, "extended_entities": {"properties": {"media": {"properties": {"display_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "expanded_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "indices": {"type": "long"}, "media_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "media_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "sizes": {"properties": {"large": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "medium": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "small": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "thumb": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}}}, "type": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}}}, "favorite_count": {"type": "long"}, "favorited": {"type": "boolean"}, "filter_level": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "is_quote_status": {"type": "boolean"}, "lang": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "possibly_sensitive": {"type": "boolean"}, "quote_count": {"type": "long"}, "reply_count": {"type": "long"}, "retweet_count": {"type": "long"}, "retweeted": {"type": "boolean"}, "source": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "text": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "timestamp_ms": {"type": "date", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "truncated": {"type": "boolean"}, "user": {"properties": {"contributors_enabled": {"type": "boolean"}, "created_at": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "default_profile": {"type": "boolean"}, "default_profile_image": {"type": "boolean"}, "description": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "favourites_count": {"type": "long"}, "followers_count": {"type": "long"}, "friends_count": {"type": "long"}, "geo_enabled": {"type": "boolean"}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "is_translator": {"type": "boolean"}, "lang": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "listed_count": {"type": "long"}, "location": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_image_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_image_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_tile": {"type": "boolean"}, "profile_banner_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_image_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_image_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_link_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_sidebar_border_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_sidebar_fill_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_text_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_use_background_image": {"type": "boolean"}, "protected": {"type": "boolean"}, "screen_name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "statuses_count": {"type": "long"}, "time_zone": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "translator_type": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "utc_offset": {"type": "long"}, "verified": {"type": "boolean"}}}}}}}
        '''
    # {"mappings": {"my_doc_type": {"properties": {"created_at": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "display_text_range": {"type": "long"}, "entities": {"properties": {"hashtags": {"properties": {"indices": {"type": "long"}, "text": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}, "media": {"properties": {"display_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "expanded_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "indices": {"type": "long"}, "media_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "media_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "sizes": {"properties": {"large": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "medium": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "small": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "thumb": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}}}, "type": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}, "urls": {"properties": {"display_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "expanded_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "indices": {"type": "long"}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}}}, "extended_entities": {"properties": {"media": {"properties": {"display_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "expanded_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "indices": {"type": "long"}, "media_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "media_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "sizes": {"properties": {"large": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "medium": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "small": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "thumb": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}}}, "type": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}}}, "favorite_count": {"type": "long"}, "favorited": {"type": "boolean"}, "filter_level": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "is_quote_status": {"type": "boolean"}, "lang": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "possibly_sensitive": {"type": "boolean"}, "quote_count": {"type": "long"}, "reply_count": {"type": "long"}, "retweet_count": {"type": "long"}, "retweeted": {"type": "boolean"}, "source": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "text": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "timestamp_ms": {"type": "date", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "truncated": {"type": "boolean"}, "user": {"properties": {"contributors_enabled": {"type": "boolean"}, "created_at": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "default_profile": {"type": "boolean"}, "default_profile_image": {"type": "boolean"}, "description": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "favourites_count": {"type": "long"}, "followers_count": {"type": "long"}, "friends_count": {"type": "long"}, "geo_enabled": {"type": "boolean"}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "is_translator": {"type": "boolean"}, "lang": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "listed_count": {"type": "long"}, "location": {"type": "geo_point", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_image_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_image_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_tile": {"type": "boolean"}, "profile_banner_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_image_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_image_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_link_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_sidebar_border_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_sidebar_fill_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_text_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_use_background_image": {"type": "boolean"}, "protected": {"type": "boolean"}, "screen_name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "statuses_count": {"type": "long"}, "time_zone": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "translator_type": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "utc_offset": {"type": "long"}, "verified": {"type": "boolean"}}}}}}}
    es.indices.create(index=INDEX_CONF, body=mapping)
    print(es.indices.get_mapping(index=INDEX_CONF))

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    numKeywords = int(input("Insert the number of keywords you want: "))
    i = 0
    l.start_index = 1
    keywords = []
    while i < numKeywords:
        keyword = input("Insert keyword: ")
        keywords.append(keyword)
        i = i + 1
    while True:
        print(keywords)
        stream = Stream(auth=auth, listener=StdOutListener(float(TIME_SLOT_CONF)))
        try:
            stream.filter(track=keywords)
        except:
            pass
        stream = None
        #new keyboards
        keywords = newKeywords(INDEX_CONF, DOC_TYPE_CONF, l.start_index, l.i -l.start_index)
        l.start_index = l.i
