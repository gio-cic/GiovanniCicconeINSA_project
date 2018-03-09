# Import the necessary methods from tweepy library
import math
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from elasticsearch import Elasticsearch, TransportError
import sys
import time
import urllib.request
import re
import os
import configparser
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from collections import Counter
from prettytable import PrettyTable
from textblob import TextBlob as tb

#read config file
config = configparser.ConfigParser()
config.sections()
config.read('config.ini')

INDEX_CONF = config['DEFAULT']['index']
DOC_TYPE_CONF = config['DEFAULT']['doc_type']
HOST_CONF = config['DEFAULT']['host']
PORT_CONF = config['DEFAULT']['port']
IMAGE_DIRECTORY_CONF = config['DEFAULT']['image_directory']
TIME_SLOT_CONF = config['DEFAULT']['time_slot_of_1_document']
NUMBER_DOCS_CONF = config['DEFAULT']['number_documents_of_1_window']

# Variables that contains the user credentials to access Twitter API
access_token = "622059145-UNN7hFb8bmEPNB4sp8AbDwrrlInMSQ7DU1EuMNtk"
access_token_secret = "1IaarCwEUCVWwBk1lhHsYp3QoRnOokt3xo6SECLbz9Ciy"
consumer_key = "qOtasggxIgJOLZYHblQRlB1gu"
consumer_secret = "d6f2QHHg13ffbHS5YXhbJhXjPzusGLHWGXJtmnCDwjlm79ddsV"

stop_words = []
def load_stopwords():
    stop_words = stopwords.words('english')
    stop_words.extend(['this','that','the','might','have','been','from',
                'but','they','will','has','having','had','how','went'
                'were','why','and','still','his','her','was','its','per','cent',
                'a','able','about','across','after','all','almost','also','am','among',
                'an','and','any','are','as','at','be','because','been','but','by','can',
                'cannot','could','dear','did','do','does','either','else','ever','every',
                'for','from','get','got','had','has','have','he','her','hers','him','his',
                'how','however','i','if','in','into','is','it','its','just','least','let',
                'like','likely','may','me','might','most','must','my','neither','nor',
                'not','of','off','often','on','only','or','other','our','own','rather','said',
                'say','says','she','should','since','so','some','than','that','the','their',
                'them','then','there','these','they','this','tis','to','too','twas','us',
                'wants','was','we','were','what','when','where','which','while','who',
                'whom','why','will','with','would','yet','you','your','ve','re','rt', 'retweet', '#fuckem', '#fuck',
                'fuck', 'ya', 'yall', 'yay', 'youre', 'youve', 'ass','factbox', 'com', '&lt', 'th',
                'retweeting', 'dick', 'fuckin', 'shit', 'via', 'fucking', 'shocker', 'wtf', 'hey', 'ooh', 'rt&amp', '&amp',
                '#retweet', 'retweet', 'goooooooooo', 'hellooo', 'gooo', 'fucks', 'fucka', 'bitch', 'wey', 'sooo', 'helloooooo', 'lol', 'smfh'])
    stop_words = set(stop_words)
    return stop_words

def normalize_text(text):
	try:
		text = text.decode('utf-8')
	except: pass
	text = re.sub('((www\.[^\s]+)|(https?://[^\s]+)|(pic\.twitter\.com/[^\s]+))','', text)
	text = re.sub('@[^\s]+','', text)
	text = re.sub('#([^\s]+)', '', text)
	text = re.sub('[:;>?<=*+()/,\-#!$%\{˜|\}\[^_\\@\]1234567890’‘]',' ', text)
	text = re.sub('[\d]','', text)
	text = text.replace(".", '')
	text = text.replace("'", ' ')
	text = text.replace("\"", ' ')
	#text = text.replace("-", " ")
	#normalize some utf8 encoding
	text = text.replace("\x9d",' ').replace("\x8c",' ')
	text = text.replace("\xa0",' ')
	text = text.replace("\x9d\x92", ' ').replace("\x9a\xaa\xf0\x9f\x94\xb5", ' ').replace("\xf0\x9f\x91\x8d\x87\xba\xf0\x9f\x87\xb8", ' ').replace("\x9f",' ').replace("\x91\x8d",' ')
	text = text.replace("\xf0\x9f\x87\xba\xf0\x9f\x87\xb8",' ').replace("\xf0",' ').replace('\xf0x9f','').replace("\x9f\x91\x8d",' ').replace("\x87\xba\x87\xb8",' ')
	text = text.replace("\xe2\x80\x94",' ').replace("\x9d\xa4",' ').replace("\x96\x91",' ').replace("\xe1\x91\xac\xc9\x8c\xce\x90\xc8\xbb\xef\xbb\x89\xd4\xbc\xef\xbb\x89\xc5\xa0\xc5\xa0\xc2\xb8",' ')
	text = text.replace("\xe2\x80\x99s", " ").replace("\xe2\x80\x98", ' ').replace("\xe2\x80\x99", ' ').replace("\xe2\x80\x9c", " ").replace("\xe2\x80\x9d", " ")
	text = text.replace("\xe2\x82\xac", " ").replace("\xc2\xa3", " ").replace("\xc2\xa0", " ").replace("\xc2\xab", " ").replace("\xf0\x9f\x94\xb4", " ").replace("\xf0\x9f\x87\xba\xf0\x9f\x87\xb8\xf0\x9f", "")
	return text


def nltk_tokenize(text):
    tokens = []
    stringReturned=""
    try:
        tokens = text.split()
        for word in tokens:
            if word.lower() not in stop_words and len(word) > 1 and word.lower().isalpha():
                stringReturned = stringReturned + " " + word.lower()
    except:
        pass
    return stringReturned


def parse_json_tweet(tweet):
    # print line
    if tweet['lang'] != 'en':
        # print "non-english tweet:", tweet['lang'], tweet
        return None

    date = tweet['created_at']
    id = tweet['id']
    if 'retweeted_status' in tweet:
        if 'extended_tweet' in tweet['retweeted_status']:
            text = tweet['retweeted_status']['extended_tweet']['full_text']
        else:
            text = tweet['retweeted_status']['text']
    else:
        if 'extended_tweet' in tweet:
            text = tweet['extended_tweet']['full_text']
        else:
            text = tweet['text']

    text = nltk_tokenize(normalize_text(text))
    return [date, id, text]

# Listener that loads the tweets in an elasticsearch index
class StdOutListener(StreamListener):
    start_index = 1
    i = 1
    document_index = 0
    documents = [None]*int(NUMBER_DOCS_CONF)
    def __init__(self, time_limit=100):
        self.start_time = time.time()
        self.limit = time_limit
        super(StdOutListener, self).__init__()

    def on_error(self, status):
        print("there is an error" + status)

    def on_status(self, status):
        if (time.time() - self.start_time) < self.limit:
            try:
                # print(status.author.screen_name, status.created_at)
                json_data =status._json

                json_data =parse_json_tweet(json_data)
                if json_data is not None:
                    json_data = json_data[2]
                    es.create(index=INDEX_CONF, doc_type=DOC_TYPE_CONF, id=StdOutListener.i, body=status._json, ignore=400)
                    self.documents[self.document_index].append(json_data)
                #print(str(StdOutListener.i)+" "+str(json_data))
                    StdOutListener.i = StdOutListener.i + 1

                #tweet_id = json_data['id']
                #try:
                #    array_media = json_data['extended_entities']['media']
                #    counter_media = 0
                #    for media in array_media:
                #        if media['type'] == 'photo':
                #            #print("image saved")
                #            url = media['media_url']
                #            #urllib.request.urlretrieve(url, IMAGE_DIRECTORY_CONF + "/" + str(tweet_id) + "_" + str(counter_media))
                #            counter_media = counter_media + 1
                #except:
                    #pass
            except:
                print("Unexpected error:", sys.exc_info()[1])
                pass
            return True
        else:
            return False

    def newKeywords(self):
        bloblist = []
        for i in range(0,int(NUMBER_DOCS_CONF)):
            bloblist.append(tb(str(self.documents[i])))

        for i, blob in enumerate(bloblist):
            print("Top words in document {}".format(i + 1))
            scores = {word: tfidf(word, blob, bloblist) for word in blob.words}
            sorted_words = sorted(scores.items(), key=lambda x: x[1], reverse=True)
            for word, score in sorted_words[:3]:
                print("\tWord: {}, TF-IDF: {}".format(word, round(score, 5)))

def tf(word, blob):
    return blob.words.count(word) / len(blob.words)

def n_containing(word, bloblist):
    return sum(1 for blob in bloblist if word in blob.words)

def idf(word, bloblist):
    return math.log(len(bloblist) / (1 + n_containing(word, bloblist)))

def tfidf(word, blob, bloblist):
    return tf(word, blob) * idf(word, bloblist)



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
    #print(es.indices.get_mapping(index=INDEX_CONF))

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
        l.document_index = 0
        for i in range(0,int(NUMBER_DOCS_CONF)):
            print(l.document_index)
            l.documents[l.document_index] = []
            print(keywords)
            stream = Stream(auth=auth, listener=StdOutListener(float(TIME_SLOT_CONF)))
            try:
                stream.filter(track=keywords)
            except:
                pass
            l.document_index = (l.document_index + 1) % int(NUMBER_DOCS_CONF)
            stream = None
        #new keywords
        l.newKeywords()
        exit(2)

