# What this code does:
# Given a Twitter stream in the format [tweet_unixtime, tweet_gmttime, tweet_id, text, hashtags, users, urls, media_urls, nfollowers, nfriends], the time window size in minutes (e.g., 15 minutes)
# and the output file name, extract top 10 topics detected in the time window
#all these parameters are set in config.ini

import codecs
from collections import Counter
from datetime import datetime
import fastcluster
import numpy as np
import re
import scipy.cluster.hierarchy as sch
from sklearn.feature_extraction.text import CountVectorizer
from sklearn import preprocessing
from sklearn.metrics.pairwise import pairwise_distances
import data_cleaning.snow as d_cl

def snow(inputfile, outputfile, stop_words, tokenizer,language = 'it', time_window_mins = 15.):

    # file_timeordered_news = codecs.open(sys.argv[3], 'r', 'utf-8')
    fout = codecs.open(outputfile, 'w')
    debug = 0

    # read tweets in time order and window them
    tweet_unixtime_old = -1
    # fout.write("time window size in mins: " + str(time_window_mins))
    tid_to_raw_tweet = {}
    window_corpus = []
    tid_to_urls_window_corpus = {}
    tids_window_corpus = []
    dfVocTimeWindows = {}
    t = 0
    ntweets = 0

    #file_timeordered_tweets = codecs.open(inputfile, 'r')
    import csv
    with open(inputfile, 'r') as input_file:
        csv_reader = csv.reader(input_file, delimiter="\t")
        header = next(csv_reader)
        for line in csv_reader:
            print(line)
            '''print(line[text_column_index])
            print(line[date_column_index])'''

            #	fout.write("\n--------------------start time window tweets--------------------\n")
            # efficient line-by-line read of big files
            [tweet_unixtime, tweet_gmttime, tweet_id, text, hashtags, users, urls, media_urls, nfollowers, nfriends] = line
            # fout.write("\n"+ str([tweet_unixtime, tweet_gmttime, tweet_id, text, hashtags, users, urls, media_urls, nfollowers, nfriends]) + "\n")
            tweet_unixtime = int(tweet_unixtime)
            hashtags = eval(hashtags)
            users = eval(users)
            urls = eval(urls)
            media_urls = eval(media_urls)
            nfollowers = int(nfollowers)
            nfriends = int(nfriends)
            if tweet_unixtime_old == -1:
                tweet_unixtime_old = tweet_unixtime

            #  		#while this condition holds we are within the given size time window
            if (tweet_unixtime - tweet_unixtime_old) < time_window_mins * 60:
                ntweets += 1
                features = d_cl.process_json_tweet(text, debug)

                tweet_bag = ""
                try:
                    for user in set(users):
                        tweet_bag += "@" + user.lower() + ","
                    for tag in set(hashtags):
                        if tag.lower() not in stop_words:
                            tweet_bag += "#" + tag.lower() + ","
                    for feature in features:
                        if feature.lower() not in stop_words:
                            tweet_bag += feature + ","
                except:
                    # print "tweet_bag error!", tweet_bag, len(tweet_bag.split(","))
                    pass

                # print tweet_bag.decode('utf-8')
                if len(users) < 3 and len(hashtags) < 3 and len(features) > 3 and len(tweet_bag.split(",")) > 4 and not str(
                        features).upper() == str(features):
                    tweet_bag = tweet_bag[:-1]
                    # fout.write(tweet_bag + "\n\n")
                    window_corpus.append(tweet_bag)
                    tids_window_corpus.append(tweet_id)
                    tid_to_urls_window_corpus[tweet_id] = media_urls
                    tid_to_raw_tweet[tweet_id] = text
                #print(tweet_bag)
                # print urls_window_corpus
            else:

                #print(window_corpus)

                dtime = datetime.fromtimestamp(tweet_unixtime_old).strftime("%d-%m-%Y %H:%M")
                print("\nWindow Starts GMT Time:", dtime, "\n")
                tweet_unixtime_old = tweet_unixtime
                # dtime = datetime.fromtimestamp(tweet_unixtime_old).strftime("%d-%m-%Y %H:%M")
                # print "\nWindow Ends GMT Time:", dtime
                # print "len(window_corpus):", len(window_corpus)
                # fout.write("\n--------------------end time window tweets--------------------\n")

                # increase window counter
                t += 1
                # sys.exit()
                # get sparse matrix X for sample vs features
                # print window_corpus
                # print urls_window_corpus
                # X = vectorizer.fit_transform(articles_corpus)
                # X = vectorizer.fit_transform(articles_corpus + window_corpus)

                # first only cluster tweets
                #				vectorizer = CountVectorizer(tokenizer=custom_tokenize_text, binary=True, min_df=max(int(len(window_corpus)*0.005), 10), ngram_range=(2,3))
                vectorizer = CountVectorizer(tokenizer=tokenizer, binary=True, min_df=max(int(len(window_corpus) * 0.0025), 10), ngram_range=(2, 3))
                #vectorizer = CountVectorizer(tokenizer=d_cl.custom_tokenize_text, binary=True,min_df=0, ngram_range=(2, 3)) #Convert a collection of text documents to a matrix of token counts
                X = vectorizer.fit_transform(window_corpus) #Learn the vocabulary dictionary and return term-document matrix.
                print(X)

                map_index_after_cleaning = {}
                Xclean = np.zeros((1, X.shape[1]))
                for i in range(0, X.shape[0]):
                    # keep sample with size at least 5
                    if X[i].sum() > 4:
                        Xclean = np.vstack([Xclean, X[i].toarray()])
                        map_index_after_cleaning[Xclean.shape[0] - 2] = i
                #   					else:
                #   						print "OOV tweet:"
                #  	 				print map_index_after_cleaning

                Xclean = Xclean[1:, ]
                # print "len(articles_corpus):", len(articles_corpus)
                print("total tweets in window:", ntweets)
                # print "len(window_corpus):", len(window_corpus)
                print("X.shape:", X.shape)
                print("Xclean.shape:", Xclean.shape)
                # print map_index_after_cleaning
                # play with scaling of X
                X = Xclean
                Xdense = np.matrix(X).astype('float')
                X_scaled = preprocessing.scale(Xdense)
                X_normalized = preprocessing.normalize(X_scaled, norm='l2')   #Scale input vectors individually to unit norm (vector length).
                # transpose X to get features on the rows
                # Xt = X_scaled.T
                # 				#print "Xt.shape:", Xt.shape
                vocX = vectorizer.get_feature_names()
                # print "Vocabulary (tweets):", vocX
                # sys.exit()

                #print(X_scaled)
                #print(X_normalized)
                #print(vocX)
                '''boost_entity = {}
                pos_tokens = CMUTweetTagger.runtagger_parse([term.upper() for term in vocX])
                # print "detect entities", pos_tokens
                for l in pos_tokens:
                    term = ''
                    for gr in range(0, len(l)):
                        term += l[gr][0].lower() + " "
                    if "^" in str(l):
                        boost_entity[term.strip()] = 2.5
                    else:
                        boost_entity[term.strip()] = 1.0
                # 				print "boost_entity",  sorted( ((v,k) for k,v in boost_entity.iteritems()), reverse=True)
    
                #  				boost_term_in_article = {}
                #  				for term in vocX:
                #   					if term in vocA:
                #  						#print "boost term in article:", term, vocA
                #  						boost_term_in_article[term] = 1.5
                #  					else:
                #  						boost_term_in_article[term] = 1.0
                #  				print "boost_term_in_article", sorted( ((v,k) for k,v in boost_term_in_article.iteritems()), reverse=True)'''

                dfX = X.sum(axis=0)
                #print ("dfX:", dfX)
                dfVoc = {}
                wdfVoc = {}
                boosted_wdfVoc = {}
                keys = vocX
                vals = dfX
                for k, v in zip(keys, vals):
                    dfVoc[k] = v
                for k in dfVoc:
                    try:
                        dfVocTimeWindows[k] += dfVoc[k]
                        avgdfVoc = (dfVocTimeWindows[k] - dfVoc[k]) / (t - 1)
                    # avgdfVoc = (dfVocTimeWindows[k] - dfVoc[k])
                    except:
                        dfVocTimeWindows[k] = dfVoc[k]
                        avgdfVoc = 0

                    wdfVoc[k] = (dfVoc[k] + 1) / (np.log(avgdfVoc + 1) + 1)
                    try:
                        boosted_wdfVoc[k] = wdfVoc[k] * 1
                    except:
                        boosted_wdfVoc[k] = wdfVoc[k]
                # 					try:
                # 						print "\ndfVoc:", k.decode('utf-8'), dfVoc[k]
                # 						print "dfVocTimeWindows:", k.decode('utf-8'), dfVocTimeWindows[k]
                # 						print "avgdfVoc:", k.decode('utf-8'), avgdfVoc
                # 						print "np.log(avgdfVoc + 1):", k.decode('utf-8'), np.log(avgdfVoc + 1)
                # 						print "wdfVoc:", k.decode('utf-8'), wdfVoc[k]
                # 						print "wdfVoc*boost_entity:", k.decode('utf-8'), wdfVoc[k] * boost_entity[k]
                #  					except: pass

                # 				print "total VocTimeWindows so far:", len(dfVocTimeWindows)
                print("sorted wdfVoc*boost_entity:")
                print(sorted(((v, k) for k, v in boosted_wdfVoc.items()), reverse=True))
                #clustering
                # Hclust: fast hierarchical clustering with fastcluster
                # X is samples by features
                # distMatrix is sample by samples distances
                #				distMatrix = pairwise_distances(X_normalized)
                distMatrix = pairwise_distances(X_normalized, metric='cosine') #A distance matrix D such that D_{i, j} is the distance between the ith and jth vectors of the given matrix X,
                #print(distMatrix)

                # print distMatrix
                # distMatrixXt = pairwise_distances(Xt)
                # print "distMatrixXt.shape", distMatrixXt.shape
                # cluster tweets
                L = fastcluster.linkage(distMatrix, method='average')  #The output of linkage is stepwise dendrogram
                '''
                The output of linkage is stepwise dendrogram, which is represented as an (N − 1) ×
                4 NumPy array with floating point entries (dtype=numpy.double). The first two
                columns contain the node indices which are joined in each step. The input nodes are
                labeled 0, . . . , N − 1, and the newly generated nodes have the labels N, . . . , 2N − 2.
                The third column contains the distance between the two nodes at each step, ie. the
                current minimal distance at the time of the merge. The fourth column counts the
                number of points which comprise each new node.
                '''
                #print(L)
                # for dt in [0.3, 0.4, 0.5, 0.6, 0.7]:
                # for dt in [0.5]:
                dt = 0.5
                print("hclust cut threshold:", dt)
                #				indL = sch.fcluster(L, dt, 'distance')
                indL = sch.fcluster(L, dt * distMatrix.max(), 'distance')  #Forms flat clusters from the hierarchical clustering defined by the linkage matrix
                #print ("indL:", indL)
                freqTwCl = Counter(indL)
                print("n_clusters:", len(freqTwCl))
                print(freqTwCl)

                npindL = np.array(indL)
                print("top most populated clusters, down to size= ", max(10, int(X.shape[0]*0.0025)))

                freq_th = max(10, int(X.shape[0] * 0.0025))
                cluster_score = {}
                for clfreq in freqTwCl.most_common(50):
                    cl = clfreq[0]
                    freq = clfreq[1]
                    cluster_score[cl] = 0
                    if freq >= freq_th:
                        # print "\n(cluster, freq):", clfreq
                        clidx = (npindL == cl).nonzero()[0].tolist()
                        cluster_centroid = X[clidx].sum(axis=0)
                        # print "centroid_array:", cluster_centroid
                        try:
                            # orig_tweet = window_corpus[map_index_after_cleaning[i]].decode("utf-8")
                            cluster_tweet = vectorizer.inverse_transform(cluster_centroid)   #Return terms per document with nonzero entries in X
                            # print orig_tweet, cluster_tweet, urls_window_corpus[map_index_after_cleaning[i]]
                            # print orig_tweet
                            # print "centroid_tweet:", cluster_tweet
                            for term in np.nditer(cluster_tweet):
                                # print "term:", term#, wdfVoc[term]

                                try:
                                    cluster_score[cl] = max(cluster_score[cl], boosted_wdfVoc[str(term).strip()])
                                # cluster_score[cl] += wdfVoc[str(term).strip()] * boost_entity[str(term)] #* boost_term_in_article[str(term)]
                                # cluster_score[cl] = max(cluster_score[cl], wdfVoc[str(term).strip()] * boost_term_in_article[str(term)])
                                # cluster_score[cl] = max(cluster_score[cl], wdfVoc[str(term).strip()] * boost_entity[str(term)])
                                # cluster_score[cl] = max(cluster_score[cl], wdfVoc[str(term).strip()] * boost_entity[str(term)] * boost_term_in_article[str(term)])
                                except:
                                    pass
                        except:
                            pass
                        cluster_score[cl] /= freq
                    else:
                        break

                sorted_clusters = sorted(((v, k) for k, v in cluster_score.items()), reverse=True)
                print("sorted cluster_score:")
                print(sorted_clusters)

                ntopics = 20
                headline_corpus = []
                orig_headline_corpus = []
                headline_to_cluster = {}
                headline_to_tid = {}
                cluster_to_tids = {}
                for score, cl in sorted_clusters[:ntopics]:
                    # print "\n(cluster, freq):", cl, freqTwCl[cl]
                    clidx = (npindL == cl).nonzero()[0].tolist()
                    # cluster_centroid = X[clidx].sum(axis=0)
                    # centroid_tweet = vectorizer.inverse_transform(cluster_centroid)
                    # random.seed(0)
                    # sample_tweets = random.sample(clidx, 3)
                    # keywords = vectorizer.inverse_transform(cluster_centroid.tolist())
                    first_idx = map_index_after_cleaning[clidx[0]]
                    keywords = window_corpus[first_idx]
                    orig_headline_corpus.append(keywords)
                    headline = ''
                    for k in keywords.split(","):
                        if not '@' in k and not '#' in k:
                            headline += k + ","
                    headline_corpus.append(headline[:-1])
                    headline_to_cluster[headline[:-1]] = cl
                    headline_to_tid[headline[:-1]] = tids_window_corpus[first_idx]
                    # 						meta_tweet = ''
                    # 						for term in np.nditer(centroid_tweet):
                    # 							meta_tweet += str(term) + ","
                    #  						headline_corpus.append(meta_tweet[:-1])
                    tids = []
                    for i in clidx:
                        idx = map_index_after_cleaning[i]
                        tids.append(tids_window_corpus[idx])
                    #   						try:
                    #   							print window_corpus[map_index_after_cleaning[i]]
                    #   						except: pass
                    cluster_to_tids[cl] = tids
                #    					try:
                # # #  						print vectorizer.inverse_transform(X[clidx[0]])
                #    						print keywords
                # # # 						print tid_to_raw_tweet[tids_window_corpus[first_idx]]
                # # # # # 							#print meta_tweet
                # # # # # 								#print "[", headline, "\t", keywords, "\t", tids, "\t", turls, "]"
                # # # # # 								#print tweet_time_window_corpus[idx], tweet_id_window_corpus[idx], window_corpus[idx].decode("utf-8")
                #    					except: pass
                # print headline_to_cluster

                ## cluster headlines to avoid topic repetition
                headline_vectorizer = CountVectorizer(tokenizer=tokenizer, binary=True, min_df=1,
                                                      ngram_range=(1, 1))
                # headline_vectorizer = TfidfVectorizer(tokenizer=custom_tokenize_text, min_df=1, ngram_range=(1,1))
                H = headline_vectorizer.fit_transform(headline_corpus)
                print("H.shape:", H.shape)
                vocH = headline_vectorizer.get_feature_names()
                # print "Voc(headline_corpus):", vocH

                Hdense = np.matrix(H.todense()).astype('float')
                # Ht = Hdense.T
                # print "Ht.shape:", Ht.shape
                # Hdense = Ht
                # 			  	distH = pairwise_distances(Hdense, metric='manhattan')
                distH = pairwise_distances(Hdense, metric='cosine')
                # distHt = pairwise_distances(Ht, metric='manhattan')
                # print distH
                #				print "fastcluster, avg, euclid"
                HL = fastcluster.linkage(distH, method='average')
                dtH = 1.0
                indHL = sch.fcluster(HL, dtH * distH.max(), 'distance')
                #				indHL = sch.fcluster(HL, dtH, 'distance')
                freqHCl = Counter(indHL)
                print("hclust cut threshold:", dtH)
                print ("n_clusters:", len(freqHCl))
                print(freqHCl)

                npindHL = np.array(indHL)
                hcluster_score = {}
                for hclfreq in freqHCl.most_common(ntopics):
                    hcl = hclfreq[0]
                    hfreq = hclfreq[1]
                    hcluster_score[hcl] = 0
                    hclidx = (npindHL == hcl).nonzero()[0].tolist()
                    for i in hclidx:
                        # print vocH[i]
                        # print headline_corpus[i]
                        # print headline_to_cluster[headline_corpus[i]]
                        # hcluster_score[hcl] += cluster_score[headline_to_cluster[headline_corpus[i]]]
                        hcluster_score[hcl] = max(hcluster_score[hcl],
                                                  cluster_score[headline_to_cluster[headline_corpus[i]]])
                #					hcluster_score[hcl] /= freq
                sorted_hclusters = sorted(((v, k) for k, v in hcluster_score.items()), reverse=True)
                print("sorted hcluster_score:")
                print(sorted_hclusters)
                for hscore, hcl in sorted_hclusters[:10]:
                    #					print "\n(cluster, freq):", hcl, freqHCl[hcl]
                    hclidx = (npindHL == hcl).nonzero()[0].tolist()
                    clean_headline = ''
                    raw_headline = ''
                    keywords = ''
                    tids_set = set()
                    tids_list = []
                    urls_list = []
                    selected_raw_tweets_set = set()
                    tids_cluster = []
                    for i in hclidx:

                        clean_headline += headline_corpus[i].replace(",", " ") + "//"
                        keywords += orig_headline_corpus[i].lower() + ","
                        tid = headline_to_tid[headline_corpus[i]]
                        tids_set.add(tid)
                        raw_tweet = tid_to_raw_tweet[tid].replace("\n", ' ').replace("\t", ' ')
                        raw_tweet = re.sub('((www\.[^\s]+)|(https?://[^\s]+)|(pic\.twitter\.com/[^\s]+))', '', raw_tweet)
                        selected_raw_tweets_set.add(raw_tweet.strip())
                        # fout.write("\nheadline tweet: " + raw_tweet.decode('utf8', 'ignore'))
                        tids_list.append(tid)
                        if tid_to_urls_window_corpus[tid]:
                            urls_list.append(tid_to_urls_window_corpus[tid])
                        for id in cluster_to_tids[headline_to_cluster[headline_corpus[i]]]:
                            tids_cluster.append(id)

                    raw_headline = tid_to_raw_tweet[headline_to_tid[headline_corpus[hclidx[0]]]]
                    raw_headline = re.sub('((www\.[^\s]+)|(https?://[^\s]+)|(pic\.twitter\.com/[^\s]+))', '', raw_headline)
                    raw_headline = raw_headline.replace("\n", ' ').replace("\t", ' ')
                    keywords_list = str(sorted(list(set(keywords[:-1].split(",")))))[1:-1].replace('u\'',
                                                                                                                     '').replace(
                        '\'', '')
                    try:
                        print
                        ("\n", clean_headline.decode('utf8', 'ignore'))  # , "\t", keywords_list
                    # 					 	print "\n", raw_headline.decode('utf8', 'ignore')
                    # 	 				 	print keywords_list.decode('utf8', 'ignore')
                    # 						print htid
                    # 						print hurl
                    except:
                        pass

                    # 					fout.write("\n\nWindow Starts GMT Time:" + str(dtime) + "\n")
                    # 					fout.write("\n\n" + raw_headline.decode('utf8', 'ignore'))
                    # 					fout.write("\n" + keywords_list.decode('utf8', 'ignore'))
                    # # 					for tid in tids_list:
                    # # 						fout.write("\n"+ tid_to_raw_tweet[tid].encode('utf8', 'replace').replace("\n", ' ').replace("\t", ' ').decode('utf8', 'ignore'))
                    # 					fout.write("\n"+ str(tids_list)[1:-1])
                    # 					#fout.write("\n" + str(urls_list)[1:-1])
                    urls_set = set()
                    for url_list in urls_list:
                        for url in url_list:
                            urls_set.add(url)
                        # break
                    # 					fout.write("\n" + str(list(urls_set))[1:-1][2:-1])

                    fout.write(
                        "\n" + str(dtime) + "\t" + raw_headline+ "\t" + keywords_list+ "\t" + str(tids_list)[1:-1] + "\t" + str(list(urls_set))[1:-1][
                                                                                     2:-1].replace('\'', '').replace(
                            'uhttp', 'http'))

                # sys.exit()
                window_corpus = []
                tids_window_corpus = []
                tid_to_urls_window_corpus = {}
                tid_to_raw_tweet = {}
                ntweets = 0
                if t == 4:
                    dfVocTimeWindows = {}
                    t = 0

            # fout.write("\n--------------------start time window tweets--------------------\n")
            # fout.write(line)

    #file_timeordered_tweets.close()
    fout.close()


def get_popular_ngrams_in_1_window(inputDir, window_index, stop_words, tokenizer, flexibility, keywords = None):
    tweet_unixtime_old = -1
    tid_to_raw_tweet = {}
    window_corpus = []
    tid_to_urls_window_corpus = {}
    tids_window_corpus = []
    dfVocTimeWindows = {}
    t = 0
    ntweets = 0
    import csv
    with open(inputDir+"/_"+str(window_index)+".csv", 'r') as input_file:
        csv_reader = csv.reader(input_file, delimiter="\t")
        header = next(csv_reader)
        for line in csv_reader:
            [tweet_unixtime, tweet_gmttime, tweet_id, text, hashtags, users, urls, media_urls, nfollowers,
             nfriends] = line

            if keywords is not None:
                bol1 = False
                for ngram in keywords:
                    bol2 = True
                    for unigram in ngram.split():
                        bol2 = bol2 and unigram in text
                    bol1 = bol1 or bol2
                if not bol1:
                    continue

            tweet_unixtime = int(tweet_unixtime)
            hashtags = eval(hashtags)
            users = eval(users)
            urls = eval(urls)
            media_urls = eval(media_urls)
            nfollowers = int(nfollowers)
            nfriends = int(nfriends)
            if tweet_unixtime_old == -1:
                tweet_unixtime_old = tweet_unixtime

            ntweets += 1
            features = d_cl.process_json_tweet(text, 0)
            tweet_bag = ""
            try:
                for user in set(users):
                    tweet_bag += "@" + user.lower() + ","
                for tag in set(hashtags):
                    if tag.lower() not in stop_words:
                        tweet_bag += "#" + tag.lower() + ","
                for feature in features:
                    if feature.lower() not in stop_words:
                        tweet_bag += feature + ","
            except:
                pass
            if len(users) < 3 and len(hashtags) < 3 and len(features) > 3 and len(
                    tweet_bag.split(",")) > 4 and not str(
                    features).upper() == str(features):
                tweet_bag = tweet_bag[:-1]
                window_corpus.append(tweet_bag)
                tids_window_corpus.append(tweet_id)
                tid_to_urls_window_corpus[tweet_id] = media_urls
                tid_to_raw_tweet[tweet_id] = text
    t += 1
    try:
        vectorizer = CountVectorizer(tokenizer=tokenizer, binary=True,min_df=max(int(len(window_corpus) * 0.0025), 10), ngram_range=(2, 3))
        #vectorizer = CountVectorizer(tokenizer=tokenizer, ngram_range=(2, 3))
        # vectorizer = CountVectorizer(tokenizer=d_cl.custom_tokenize_text, binary=True,min_df=0, ngram_range=(2, 3)) #Convert a collection of text documents to a matrix of token counts
        X = vectorizer.fit_transform(window_corpus)  # Learn the vocabulary dictionary and return term-document matrix.
    except ValueError:
        return []
    map_index_after_cleaning = {}
    Xclean = np.zeros((1, X.shape[1]))
    for i in range(0, X.shape[0]):
        # keep sample with size at least 5
        if X[i].sum() > 4:
            Xclean = np.vstack([Xclean, X[i].toarray()])
            map_index_after_cleaning[Xclean.shape[0] - 2] = i
    #   					else:
    #   						print "OOV tweet:"
    #  	 				print map_index_after_cleaning

    Xclean = Xclean[1:, ]
    X = Xclean
    Xdense = np.matrix(X).astype('float')
    X_scaled = preprocessing.scale(Xdense)
    X_normalized = preprocessing.normalize(X_scaled,
                                           norm='l2')  # Scale input vectors individually to unit norm (vector length).
    vocX = vectorizer.get_feature_names()
    # print "Vocabulary (tweets):", vocX
    # sys.exit()

    # print(X_scaled)
    # print(X_normalized)
    # print(vocX)
    '''boost_entity = {}
    pos_tokens = CMUTweetTagger.runtagger_parse([term.upper() for term in vocX])
    # print "detect entities", pos_tokens
    for l in pos_tokens:
        term = ''
        for gr in range(0, len(l)):
            term += l[gr][0].lower() + " "
        if "^" in str(l):
            boost_entity[term.strip()] = 2.5
        else:
            boost_entity[term.strip()] = 1.0
    # 				print "boost_entity",  sorted( ((v,k) for k,v in boost_entity.iteritems()), reverse=True)

    #  				boost_term_in_article = {}
    #  				for term in vocX:
    #   					if term in vocA:
    #  						#print "boost term in article:", term, vocA
    #  						boost_term_in_article[term] = 1.5
    #  					else:
    #  						boost_term_in_article[term] = 1.0
    #  				print "boost_term_in_article", sorted( ((v,k) for k,v in boost_term_in_article.iteritems()), reverse=True)'''

    dfX = X.sum(axis=0)
    dfVoc = {}
    wdfVoc = {}
    boosted_wdfVoc = {}
    keys = vocX
    vals = dfX
    for k, v in zip(keys, vals):
        dfVoc[k] = v
    for k in dfVoc:
        try:
            dfVocTimeWindows[k] += dfVoc[k]
            avgdfVoc = (dfVocTimeWindows[k] - dfVoc[k]) / (t - 1)
        except:
            dfVocTimeWindows[k] = dfVoc[k]
            avgdfVoc = 0

        wdfVoc[k] = (dfVoc[k] + 1) / (np.log(avgdfVoc + 1) + 1)
        try:
            boosted_wdfVoc[k] = wdfVoc[k] * 1
        except:
            boosted_wdfVoc[k] = wdfVoc[k]
    mean_value =  np.array(list(boosted_wdfVoc.values())).mean()
    temp = list(sorted(((v, k) for k, v in boosted_wdfVoc.items() if v > flexibility * mean_value), reverse=True))
    #print("sorted wdfVoc*boost_entity:", resultlist[:10])
    temp = [k for (v,k) in temp[:1]]
    resultlist = list()

    for ngram in temp:
        bol = True
        for word in ngram.split():
            if(word in stop_words):
                bol = False
        if(bol):
            resultlist.append(ngram)
            print(i,"***", tweet_gmttime)
    return resultlist

def ngram_in_text(text, ngram):
    bol = True
    for unigram in ngram.split():
        bol = bol and unigram in text.split()
    return bol


def to_add_in_missed(text, proposedNgram, InitialSetKeywords):
    #proposedNgram deve essere in text
    bol = ngram_in_text(text, proposedNgram)
    for ngram in InitialSetKeywords:
        bol = bol and not ngram_in_text(text, ngram)
    return bol

def missed(number_of_windows_in_which_compute_missed , inputDir, InitialSetKeywords, proposedNgram):
    import csv
    num_missed = 0
    for window_index in range(int(number_of_windows_in_which_compute_missed)+1):
        with open(inputDir+"/_"+str(window_index)+".csv", 'r') as input_file:
            csv_reader = csv.reader(input_file, delimiter="\t")
            header = next(csv_reader)
            for line in csv_reader:
                [tweet_unixtime, tweet_gmttime, tweet_id, text, hashtags, users, urls, media_urls, nfollowers,
                 nfriends] = line
                text = text.lower()
                num_missed += to_add_in_missed(text, proposedNgram, InitialSetKeywords)
    return num_missed


#a new ngram is selected only if its value is > flexibility * avg(values in that window)
def get_popular_ngrams_in_all_windows(language, number_of_windows, inputDir, flexibility, keywords):
    I_kw_S = keywords
    stop_words = set()
    with open("C:/Users/giovanni/PycharmProjects/GiovanniCicconeINSA_project/data_cleaning/stopwords/twitter_"+language+".txt", 'r') as input_file:
        for line in input_file.readlines():
            stop_words.add(line.strip('\n'))
    proposed_keywords = list()
    for i in range(number_of_windows):
        popular_ngrams_in_window = get_popular_ngrams_in_1_window(inputDir, i, stop_words, d_cl.custom_tokenize_text, flexibility = flexibility, keywords= I_kw_S)
        for ngram in popular_ngrams_in_window:
            if ngram not in proposed_keywords:
                proposed_keywords.append(ngram)
                print(ngram)

                print("missed ",str(missed(number_of_windows_in_which_compute_missed = i, inputDir=inputDir, InitialSetKeywords = I_kw_S, proposedNgram = ngram)))
                print("\n")
                break

if __name__ == '__main__':
    get_popular_ngrams_in_all_windows("fr", 175, "C:/Users/giovanni/PycharmProjects/GiovanniCicconeINSA_project/snow_windowses_4", 1.0,
                                      keywords = ["fn", "barrage" , "score", "médias", "faut", "ps", "dû", "théâtrale", "umps:il", "emplois"])

