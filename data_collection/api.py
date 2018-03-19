from data_collection.collect_tweets import collect_tweets
from data_collection.elasticsearch_management import  elasticsearch_management
import json


def from_tweets_to_elasticsearch(minute_collecting, keywords, index, doctype, host, port):
    es = elasticsearch_management(host=host, port=port)
    es.create_mapping_and_index(index=index, doctype=doctype)
    collect_tweets(elasticsearch_management=es, minutes_collecting=minute_collecting, keywords=keywords)



def extract_fields(line, fields, language):
    import time
    import datetime
    try:
        tweet = json.loads(line)
        if tweet['lang'] != language:
            return None

        lineOut = list()

        if 'date' in fields:
            date = date = datetime.datetime.strptime(tweet['created_at'].replace("+0000", ''), '%a %b %d %H:%M:%S %Y').strftime("%Y-%m-%d %H:%M:%S")
            lineOut.append(date)
        if 'tweet_gmttime' in fields:
            tweet_gmttime = tweet['created_at']
            lineOut.append(tweet_gmttime)

        if 'tweet_unixtime' in fields:
            try:
                c = time.strptime(tweet_gmttime.replace("+0000", ''), '%a %b %d %H:%M:%S %Y')
            except:
                pass
            tweet_unixtime = int(time.mktime(c))
            lineOut.insert(0, tweet_unixtime)

        if 'tweet_id' in fields:
            tweet_id = tweet['id']['$numberLong']
            lineOut.append(tweet_id)
        if 'text' in fields:
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
            lineOut.append(text)
        if 'hashtags' in fields:
            hashtags = [hashtag['text'] for hashtag in tweet['entities']['hashtags']]
            lineOut.append(hashtags)
        if 'users' in fields:
            users = [user_mention['screen_name'] for user_mention in tweet['entities']['user_mentions']]
            lineOut.append(users)
        if 'urls' in fields:
            urls = [url['expanded_url'] for url in tweet['entities']['urls']]
            lineOut.append(urls)
        if 'media_urls' in fields:
            media_urls = []
            if 'media' in tweet['entities']:
                media_urls = [media['media_url'] for media in tweet['entities']['media']]
            lineOut.append(media_urls)
        if 'nfollowers' in fields:
            nfollowers = tweet['user']['followers_count']
            lineOut.append(nfollowers)
        if 'nfriends' in fields:
            nfriends = tweet['user']['friends_count']
            lineOut.append(nfriends)
        return lineOut
    except:
        print("error")
        pass

def from_elasticsearch_to_csv(filenameOut, index, doctype, elasticsearch,
                              fields_to_extract = ['tweet_unixtime','tweet_gmttime','tweet_id','text','hashtags','users','urls','media_urls','nfollowers','nfriends'],
                              language='en'):
    WINDOW_NUM_DOC = 1000

    import csv
    with open(filenameOut, 'w', newline='') as out_file:
        writer = csv.writer(out_file,delimiter='\t')
        writer.writerow(tuple(fields_to_extract))
        numDocuments = elasticsearch.count(index=index, doc_type=doctype)['count']
        print("TOTAL NUMBER OF DOCUMENTS",numDocuments)
        i = 0
        while i < numDocuments:
            if i + WINDOW_NUM_DOC > numDocuments:
                numDocuments_toquery = numDocuments - i
            else:
                numDocuments_toquery = WINDOW_NUM_DOC
            start_index = i
            d = {}
            d["ids"] = [str(i) for i in range(start_index, numDocuments_toquery + start_index)]
            query_result = elasticsearch.mget(index=index, doc_type=doctype, body=json.dumps(d, ensure_ascii=False))

            while i < start_index + numDocuments_toquery:
                if query_result['docs'][i - start_index]['found']:
                    extracted_fields = extract_fields(json.dumps(query_result['docs'][i - start_index]['_source']), fields_to_extract, language)
                    if extracted_fields is not None:

                        try:
                            writer.writerow(extracted_fields)
                        except UnicodeEncodeError:
                            pass
                i = i + 1

def split_dataset_fixed_timewindows(inputFile, outFile, outputDir, InitialKeywordSet = None):
    import csv
    # I want to follow "fn" sub-event in FDL_2015 dataset:
    #    S_ = "fn" sub-event
    #    I_kw_S = "elezioni4marzo2018"

    #   I try to follow that subevent with your method
    I_kw_S = InitialKeywordSet
    with open(inputFile, 'r') as input_file:

        csv_reader = csv.reader(input_file, delimiter="\t")
        header = next(csv_reader)
        out_file = open(outFile, 'w', newline='')
        writer = csv.writer(out_file, delimiter='\t')
        writer.writerow(tuple("tweet_unixtime, tweet_gmttime, tweet_id, text, hashtags, users, urls, media_urls, nfollowers, nfriends".split()))
        for line in csv_reader:
            [tweet_unixtime, tweet_gmttime, tweet_id, text, hashtags, users, urls, media_urls, nfollowers,
             nfriends] = line
            text = str(text)
            bool = False
            if I_kw_S is None:
                bool = True
            else:
                for k in I_kw_S:
                    if k in text or k in hashtags:
                        bool = True
                        break
            if bool:
                writer.writerow(line)

    inputFile = outFile
    time_window_mins = 30

    import os
    import shutil
    if os.path.exists(outputDir):
        shutil.rmtree(outputDir)
    os.makedirs(outputDir)
    tweet_unixtime_old = -1
    t = 0
    import csv
    with open(inputFile, 'r') as input_file:
        csv_reader = csv.reader(input_file, delimiter="\t")
        header = next(csv_reader)
        out_file = open(outputDir+"/_" + str(t) + ".csv", 'w', newline='')
        writer = csv.writer(out_file, delimiter='\t')
        writer.writerow(tuple(
            "tweet_unixtime, tweet_gmttime, tweet_id, text, hashtags, users, urls, media_urls, nfollowers, nfriends".split()))
        for line in csv_reader:
            print(line)
            [tweet_unixtime, tweet_gmttime, tweet_id, text, hashtags, users, urls, media_urls, nfollowers,
             nfriends] = line
            tweet_unixtime = int(tweet_unixtime)
            if tweet_unixtime_old == -1:
                tweet_unixtime_old = tweet_unixtime

            #  		#while this condition holds we are within the given size time window
            if (tweet_unixtime - tweet_unixtime_old) < time_window_mins * 60:
                writer.writerow(line)
            else:
                tweet_unixtime_old = tweet_unixtime
                out_file.close()
                t += 1
                out_file = open(outputDir+"/_" + str(t) + ".csv", 'w', newline='')
                writer = csv.writer(out_file, delimiter='\t')
                writer.writerow(tuple(
                    "tweet_unixtime, tweet_gmttime, tweet_id, text, hashtags, users, urls, media_urls, nfollowers, nfriends".split()))