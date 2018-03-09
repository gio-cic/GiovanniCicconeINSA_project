
from elasticsearch import Elasticsearch
import urllib.request

if __name__ == '__main__':
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    doc=es.get(index='my_index', doc_type='my_doc_type', id=3)
    tweet_id = doc['_source']['id']
    try:
        array_media = doc['_source']['extended_tweet']['extended_entities']['media']
        counter_media=0
        for media in array_media:
            url = media['media_url']
            urllib.request.urlretrieve(url,"img/"+str(tweet_id)+"_"+str(counter_media))
            counter_media = counter_media+1
    except:
        pass