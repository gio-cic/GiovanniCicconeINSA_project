#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from elasticsearch import Elasticsearch
import sys
import time

#Variables that contains the user credentials to access Twitter API
access_token = "622059145-UNN7hFb8bmEPNB4sp8AbDwrrlInMSQ7DU1EuMNtk"
access_token_secret = "1IaarCwEUCVWwBk1lhHsYp3QoRnOokt3xo6SECLbz9Ciy"
consumer_key = "qOtasggxIgJOLZYHblQRlB1gu"
consumer_secret = "d6f2QHHg13ffbHS5YXhbJhXjPzusGLHWGXJtmnCDwjlm79ddsV"

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

#Listener that loads the tweets in an elasticsearch index
class StdOutListener(StreamListener):
    i = 1
    def on_error(self, status):
        print("there is an error"+status)

    def on_status(self, status):
        try:
            time.sleep(3)
            #print(status.author.screen_name, status.created_at)
            json_data = status._json
            es.create(index="my_index", doc_type="my_doc_type", id=StdOutListener.i, body=json_data, ignore=400)
            mapping = es.indices.get_mapping(index="my_index", doc_type="my_doc_type")
            print(mapping)
            if StdOutListener.i == 1:
                mapping['my_index']['mappings']['my_doc_type']['properties']['timestamp_ms']['type'] = 'date'  #date
                mapping['my_index']['mappings']['my_doc_type']['properties']['user']['properties']['location']['type'] ='geo_point' #geo_point
                print(mapping['my_index']['mappings']['my_doc_type']['properties']['timestamp_ms']['type'])
                print(mapping['my_index']['mappings']['my_doc_type']['properties']['user']['properties']['location']['type'])
                es.indices.delete(index='my_index')
                #es.indices.create
                es.indices.create(index="my_index", body=mapping['my_index']['mappings']['my_doc_type'])
            print(json_data)
            print(StdOutListener.i)
            StdOutListener.i = StdOutListener.i+1
        except:
            print("Unexpected error:", sys.exc_info()[1])
            pass


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    keyword1 = 'lyon'
    keyword2 = '#lyon'
    keyword3 = 'paris'
    keyword4 = '#paris'
    stream.filter(None, track=[keyword1, keyword2, keyword3, keyword4])
