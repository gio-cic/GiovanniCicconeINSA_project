import math
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from collections import Counter
from prettytable import PrettyTable
from textblob import TextBlob as tb


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


def tf(word, blob):
    return blob.words.count(word) / len(blob.words)

def n_containing(word, bloblist):
    return sum(1 for blob in bloblist if word in blob.words)

def idf(word, bloblist):
    return math.log(len(bloblist) / (1 + n_containing(word, bloblist)))

def tfidf(word, blob, bloblist):
    return tf(word, blob) * idf(word, bloblist)
if __name__ == '__main__':

    es = Elasticsearch([{'host': HOST_CONF, 'port': int(PORT_CONF)}])

    query_result= es.search(index=INDEX_CONF, doc_type=DOC_TYPE_CONF, from_="1", size="500")
    tweets=[]
    for i in range(1,500):
        try:
            text = query_result['hits']['hits'][i]['_source']['retweeted_status']['extended_tweet']['full_text']
        except:
            text = query_result['hits']['hits'][i]['_source']['text']
        print(text)
        tweets.append(text)
    print(tweets)

tweets_cleaned = []
counter = Counter()
text_tokenized = None
for text in tweets:
    text_tokenized_original = word_tokenize(text)
    text_tokenized = []
    for w in text_tokenized_original:
        tweets_cleaned.append(str(text_tokenized_original))

bloblist= []
for document in tweets_cleaned:
    bloblist.append(tb(document))

for i, blob in enumerate(bloblist):
    print("Top words in document {}".format(i + 1))
    scores = {word: tfidf(word, blob, bloblist) for word in blob.words}
    sorted_words = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    for word, score in sorted_words[:3]:
        print("\tWord: {}, TF-IDF: {}".format(word, round(score, 5)))