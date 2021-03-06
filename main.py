
from data_collection.elasticsearch_management import elasticsearch_management

def from_tweets_to_elasticsearch(minute_collecting=1, keywords=['milan, arsenal'], index='myindex', doctype='mydoctype', host='localhost', port=9200):
    from data_collection import api
    api.from_tweets_to_elasticsearch(minute_collecting=minute_collecting, keywords=keywords, index=index,
                                     doctype=doctype, host=host, port=port)

def from_elasticsearch_to_csv(filenameOut="prova.txt", index="italian_political_elections_collection_1", doctype="tweet",
                              language='it',fields_to_extract = ['tweet_unixtime','tweet_gmttime','tweet_id','text','hashtags','users','urls','media_urls','nfollowers','nfriends']):
    from data_collection import api
    em = elasticsearch_management('localhost', 9200)
    api.from_elasticsearch_to_csv(filenameOut=filenameOut, index=index, doctype=doctype,
                                  elasticsearch=em.elasticsearch, language=language, fields_to_extract=fields_to_extract)

def snow(inputfile, outputfile, language="it",
         time_window_mins=30):
    from analysis.snow import snow
    from data_cleaning import snow as d_cl
    stop_words = d_cl.load_stopwords(language)
    snow(inputfile=inputfile, outputfile=outputfile, language=language,
         time_window_mins=time_window_mins, stop_words=stop_words, tokenizer=d_cl.custom_tokenize_text)


def mabed(inputCsvFile, outputFile, outputDir, NumEventsToDetect=10, stopwordsFile="data_cleaning/stopwords/twitter_it.txt", min_absolute_frequency=10, maximumAbsoluteWordFrequency=0.4,
          timeSliceLength=30, numberCandidateWordsPerEvent=10, theta=0.6, sigma=0.6, csvSeparator ='\t'):
    import timeit
    from data_cleaning.mabed import Corpus

    i = inputCsvFile #Input csv file
    k = NumEventsToDetect#Number of top events to detect
    sw = stopwordsFile#Stop-word list
    sep =csvSeparator #CSV separator
    o = outputFile #Output pickle file
    maf = min_absolute_frequency #min_absolute_frequency
    mrf = maximumAbsoluteWordFrequency#'Maximum absolute word frequency, default to 0.4
    tsl = timeSliceLength#Time-slice length, default to 30 (minutes)
    p = numberCandidateWordsPerEvent #Number of candidate words per event, default to 10
    t = theta # Theta
    s = sigma # Sigma


    print('Corpus: %s\n   k: %d\n   Stop-words: %s\n   Min. abs. word frequency: %d\n   Max. rel. word frequency: %f' %(i, k, sw, maf, mrf))
    print('   p: %d\n   theta: %f\n   sigma: %f' % (p, t, s))

    print('Loading corpus...')
    start_time = timeit.default_timer()
    my_corpus = Corpus(i, sw, maf, mrf, sep)
    elapsed = timeit.default_timer() - start_time
    print('Corpus loaded in %f seconds.' % elapsed)

    time_slice_length = tsl
    print('Partitioning tweets into %d-minute time-slices...' % time_slice_length)
    start_time = timeit.default_timer()
    my_corpus.discretize(time_slice_length)
    elapsed = timeit.default_timer() - start_time
    print('Partitioning done in %f seconds.' % elapsed)
    import pickle
    from analysis.mabed import MABED
    print('Running MABED...')
    k = k
    p = p
    theta = t
    sigma = s
    start_time = timeit.default_timer()
    mabed = MABED(my_corpus)
    mabed.run(k=k, p=p, theta=theta, sigma=sigma)
    mabed.print_events()
    elapsed = timeit.default_timer() - start_time
    print('Event detection performed in %f seconds.' % elapsed)

    if o is not None:
        with open(o, 'wb') as output_file:
            pickle.dump(mabed, output_file)
        print('Events saved in %s' %o)

    from visualization.mabed import visualize
    visualize(outputFile,outputDir)



if __name__ == '__main__':


    #split data collection in fixed_time windowses
    from data_collection import api
    api.split_dataset_fixed_timewindows(inputFile="C:/Users/giovanni/PycharmProjects/GiovanniCicconeINSA_project/data/3_FDL2015/FDL2015_snow.csv",
                                        outFile="C:/Users/giovanni/PycharmProjects/GiovanniCicconeINSA_project/data/3_FDL2015/FDL2015_snow_2.csv",
                                        outputDir = "snow_windowses_4",
                                        InitialKeywordSet=None)
    '''
    #main example 1: collect tweets in elasticsearch
    from_tweets_to_elasticsearch(minute_collecting=60*11, keywords = ['donald trump'],
                                 index='myindex', doctype='mydoctype', host='localhost', port=9200)
    '''



    #main example 2: execute mabed on a tweets collection
    '''from_elasticsearch_to_csv(filenameOut="b.csv", index="italian_political_elections_collection_1",
                              doctype="tweet",
                              language='it',
                              fields_to_extract=['date', 'text'])'''
    #mabed(inputCsvFile="data/3_FDL2015/FDL2015_mabed.csv", outputFile="data/3_FDL2015/mabed_output.txt", outputDir ="outDir_FDL2015", stopwordsFile="data_cleaning/stopwords/twitter_fr.txt", NumEventsToDetect=5)


    #main example 3: execute snow on a tweets collection
    '''from_elasticsearch_to_csv(filenameOut="snow.csv", index="italian_political_elections_collection_1",
                              doctype="tweet",
                              language='it',
                              fields_to_extract=['tweet_unixtime', 'tweet_gmttime', 'tweet_id', 'text', 'hashtags',
                                                 'users', 'urls', 'media_urls', 'nfollowers', 'nfriends'])'''
    '''snow("C:/Users/giovanni/PycharmProjects/GiovanniCicconeINSA_project/data/3_FDL2015/FDL2015_snow.csv", "FDL2015_snow_topics.csv", language="fr", time_window_mins=100)'''