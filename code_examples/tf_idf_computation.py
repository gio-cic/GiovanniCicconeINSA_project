import math
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from collections import Counter
from prettytable import PrettyTable
from textblob import TextBlob as tb
import codecs
import text_processing
import re

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
TIME_SLOT= config['DEFAULT']['time_slot_of_1_document']
NUMBER_DOCS = config['DEFAULT']['number_documents_of_1_window']

if __name__ == '__main__':

    file_timeordered_tweets = codecs.open("tweets_formatted.txt", 'r', 'utf-8')
    time_window_mins = float(1.0)
    tweet_unixtime_old = -1
    tid_to_raw_tweet = {}
    window_corpus = []
    tid_to_urls_window_corpus = {}
    tids_window_corpus = []
    dfVocTimeWindows = {}
    t = 0
    ntweets = 0
    documents = [None] * int(NUMBER_DOCS)
    documents[0]=""
    for line in file_timeordered_tweets:
        [tweet_unixtime, tweet_gmttime, tweet_id, text] = eval(line)

        if tweet_unixtime_old == -1:
            tweet_unixtime_old = tweet_unixtime
        if (tweet_unixtime - tweet_unixtime_old) < time_window_mins * int(TIME_SLOT):
            ntweets += 1

            documents[t] = documents[t] + " " + text
        else:
            print(t)
            tweet_unixtime_old = tweet_unixtime
            t = (t + 1)% int(NUMBER_DOCS)
            if (t==0): #compute tdfif
                print(documents)
                scores = text_processing.tfidf(documents)
                print(scores)
                scores_list = scores.values()
                scores_avg = sum(scores_list)/len(scores_list)
                print(scores_avg)

                newKeywords = {k: v for k, v in scores.items() if v>scores_avg*1.2}
                print(newKeywords)
                newKeywords = sorted(newKeywords.items(), key=lambda x: x[1], reverse=True)
                print(newKeywords)
            #text_processing.tfidf(['ciao mamma', 'ciao papa'])
            documents[t] = ""
    exit(2)








    es = Elasticsearch([{'host': HOST_CONF, 'port': int(PORT_CONF)}])
    documents = [None] * int(5)

    for j in range(0,5):
        query_result= es.search(index=INDEX_CONF, doc_type=DOC_TYPE_CONF, from_=j*100+1, size="100")
        documents[j] = ""
        for i in range(0,100):
            json_data = query_result['hits']['hits'][i]['_source']
            json_data = parse_json_tweet(json_data)
            if json_data is not None:
                json_data = json_data[2]
                documents[j] = documents[j]+" "+json_data
        print(documents[j])

    bloblist= []
    for document in documents:
        bloblist.append(tb(document))

    for i, blob in enumerate(bloblist):
        print("Top words in document {}".format(i + 1))
        scores = {word: tfidf(word, blob, bloblist) for word in blob.words}
        sorted_words = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        for word, score in sorted_words[:3]:
            print("\tWord: {}, TF-IDF: {}".format(word, round(score, 5)))