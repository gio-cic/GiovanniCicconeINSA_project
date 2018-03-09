# Import the necessary methods from tweepy library
from elasticsearch import Elasticsearch, TransportError
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
INITIAL_ID = config['DEFAULT']['initial_id']
END_ID = config['DEFAULT']['end_id']

if __name__ == '__main__':

    es = Elasticsearch([{'host': HOST_CONF, 'port': int(PORT_CONF)}])

    query_result= es.search(index=INDEX_CONF, doc_type=DOC_TYPE_CONF, from_=int(INITIAL_ID), size=int(INITIAL_ID)+int(END_ID))
    tweets= []
    for i in range(int(INITIAL_ID), int(INITIAL_ID)+int(END_ID)):
        try:
            text = query_result['hits']['hits'][i]['_source']['retweeted_status']['extended_tweet']['full_text']
            tweets.append(text)
        except:
            try:
                text = query_result['hits']['hits'][i]['_source']['text']
                tweets.append(text)
            except:
                pass
        tweets.append(text)
    print(tweets)

    stopWords = set(stopwords.words('english'))
    print(stopWords)
    counter = Counter()
    text_tokenized = None
    for text in tweets:
        text_tokenized_original = word_tokenize(text)
        text_tokenized = []
        for w in text_tokenized_original:
            if w not in stopWords:
                if w not in [',', '.', ';', ':', "'", "@", "#", "https", "RT", "\"", "''", "I","OF","The","You"]:
                    if w.isalpha():
                        text_tokenized.append(w)



        for item in text_tokenized:
            counter[item] = counter[item] + 1
    print(counter.most_common()[:3])  # top 10
    pt = PrettyTable(field_names=['Word', 'Count'])
    [pt.add_row(kv) for kv in counter.most_common()[:50]]
    pt.align['Word'], pt.align['Count'] = 'l', 'r'  # Set column alignment
    print(pt)
