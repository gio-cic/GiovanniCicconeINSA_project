# text processing; supported languages are "en" "fr" "it"
import codecs
import time
import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
import re
import configparser
from sklearn.feature_extraction.text import TfidfVectorizer
import json
def load_stopwords():
    if LANGUAGE == "en":
        stop_words = stopwords.words('english')
        stop_words.extend(['this', 'that', 'the', 'might', 'have', 'been', 'from',
                           'but', 'they', 'will', 'has', 'having', 'had', 'how', 'went','were', 'why', 'and', 'still', 'his',
                           'her', 'was', 'its', 'per', 'cent',
                           'a', 'able', 'about', 'across', 'after', 'all', 'almost', 'also', 'am', 'among',
                           'an', 'and', 'any', 'are', 'as', 'at', 'be', 'because', 'been', 'but', 'by', 'can',
                           'cannot', 'could', 'dear', 'did', 'do', 'does', 'either', 'else', 'ever', 'every',
                           'for', 'from', 'get', 'got', 'had', 'has', 'have', 'he', 'her', 'hers', 'him', 'his',
                           'how', 'however', 'i', 'if', 'in', 'into', 'is', 'it', 'its', 'just', 'least', 'let',
                           'like', 'likely', 'may', 'me', 'might', 'most', 'must', 'my', 'neither', 'nor',
                           'not', 'of', 'off', 'often', 'on', 'only', 'or', 'other', 'our', 'own', 'rather', 'said',
                           'say', 'says', 'she', 'should', 'since', 'so', 'some', 'than', 'that', 'the', 'their',
                           'them', 'then', 'there', 'these', 'they', 'this', 'tis', 'to', 'too', 'twas', 'us',
                           'wants', 'was', 'we', 'were', 'what', 'when', 'where', 'which', 'while', 'who',
                           'whom', 'why', 'will', 'with', 'would', 'yet', 'you', 'your', 've', 're', 'rt', 'retweet',
                           '#fuckem', '#fuck',
                           'fuck', 'ya', 'yall', 'yay', 'youre', 'youve', 'ass', 'factbox', 'com', '&lt', 'th',
                           'retweeting', 'dick', 'fuckin', 'shit', 'via', 'fucking', 'shocker', 'wtf', 'hey', 'ooh',
                           'rt&amp', '&amp',
                           '#retweet', 'retweet', 'goooooooooo', 'hellooo', 'gooo', 'fucks', 'fucka', 'bitch', 'wey',
                           'sooo', 'helloooooo', 'lol', 'smfh'])
    elif LANGUAGE == "fr":
        stop_words = stopwords.words('french')
    elif LANGUAGE == "it":
        stop_words = stopwords.words('italian')
    stop_words = set(stop_words)
    return stop_words


def normalize_text(text):
    try:
        text = text.decode('utf-8')
    except:
        pass
    text = re.sub('((www\.[^\s]+)|(https?://[^\s]+)|(pic\.twitter\.com/[^\s]+))','', text)
    if (CONSIDER_HASHTAG == "true"):
        text = re.sub('#', ' ', text)
    else:
        text = re.sub('#([^\s]+)', '', text)
    text = re.sub('@([^\s]+)', '', text)
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


def parse_json_tweet(line):
    # print line
    tweet = json.loads(line)
    if tweet['lang'] != LANGUAGE:
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

def tfidf(corpus):
    vectorizer = TfidfVectorizer()
    X = vectorizer.fit_transform(corpus)
    idf = vectorizer.idf_
    return dict(zip(vectorizer.get_feature_names(), idf))

if __name__ == '__main__':
    # read config file
    config = configparser.ConfigParser()
    config.sections()
    config.read('config.ini')
    LANGUAGE = config['DEFAULT']['LANGUAGE']
    CONSIDER_HASHTAG = config['DEFAULT']['CONSIDER_HASHTAG']
    if (LANGUAGE != "en") and (LANGUAGE != "fr") and (LANGUAGE != "it"):
        exit("language must be 'en' or 'fr' or 'it'")
    stop_words = []
    file_timeordered_json_tweets = codecs.open("tweets.txt", 'r', 'utf-8')
    fout = codecs.open("tweets_formatted.txt", 'w', 'utf-8')
    # efficient line-by-line read of big files
    for line in file_timeordered_json_tweets:
        try:
            parsed_data = parse_json_tweet(line)
            if parsed_data is not None:
                [tweet_gmttime, tweet_id, text] = parsed_data
                try:
                    c = time.strptime(tweet_gmttime.replace("+0000", ''), '%a %b %d %H:%M:%S %Y')
                except:
                    print
                    "pb with tweet_gmttime", tweet_gmttime, line
                    pass
                tweet_unixtime = int(time.mktime(c))
                fout.write(str([tweet_unixtime, tweet_gmttime, tweet_id, text]) + "\n")
        except:
            pass
    file_timeordered_json_tweets.close()
    fout.close()


