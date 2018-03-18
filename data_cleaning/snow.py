import nltk
import re

stop_words = list()
def load_stopwords(language):
    if language == "en":
        stop_words = nltk.corpus.stopwords.words('english')
        stop_words.extend(['this', 'that', 'the', 'might', 'have', 'been', 'from',
                       'but', 'they', 'will', 'has', 'having', 'had', 'how', 'went'
                                                                             'were', 'why', 'and', 'still', 'his',
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
        # ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 'should', 'now']
        # custom stop words for avoiding retrieving too much spam from Twitter
        # 	stop_words.append("video")
        # 	stop_words.append("videos")
        # 	stop_words.append("anyone")
        # 	stop_words.append("today")
        # 	stop_words.append("new")
        # 	stop_words.append("former")
        # 	stop_words.append("cent")
        # 	stop_words.append("image")
        # 	stop_words.append("images")
        # 	stop_words.append("want")
        # 	stop_words.append("yes")
        # 	stop_words.append("no")
        # 	stop_words.append("on")
        # 	stop_words.append("dont")
        # 	stop_words.append(".")
        # 	stop_words.append("inside")
        # 	stop_words.append("first")
        # 	stop_words.append("immense")
        # 	stop_words.append("simple")
        # 	stop_words.append("finds")
        # 	stop_words.append("best")
        # 	stop_words.append("large")
        # 	stop_words.append("huge")
        # 	stop_words.append("regardless")
        # 	stop_words.append("latest")
        # 	stop_words.append("proud")
        # 	stop_words.append("as")
        # 	stop_words.append("although")
        # 	stop_words.append("...")
    elif language == "fr":
        stop_words = nltk.corpus.stopwords.words('french')
    elif language == "it":
        stop_words = nltk.corpus.stopwords.words('italian')

    # print(stop_words)

    # turn list into set for faster search
    stop_words = set(stop_words)
    return stop_words


##end load_stopwords()


def normalize_text(text):
    try:
        text = text.decode('utf-8')
    except:
        pass
    text = re.sub('((www\.[^\s]+)|(https?://[^\s]+)|(pic\.twitter\.com/[^\s]+))', '', text)
    text = re.sub('@[^\s]+', '', text)
    text = re.sub('#([^\s]+)', '', text)
    text = re.sub('[:;>?<=*+()/,\-#!$%\{˜|\}\[^_\\@\]1234567890’‘]', ' ', text)
    text = re.sub('[\d]', '', text)
    text = text.replace(".", '')
    text = text.replace("'", ' ')
    text = text.replace("\"", ' ')
    # text = text.replace("-", " ")
    # normalize some utf8 encoding
    text = text.replace("\x9d", ' ').replace("\x8c", ' ')
    text = text.replace("\xa0", ' ')
    text = text.replace("\x9d\x92", ' ').replace("\x9a\xaa\xf0\x9f\x94\xb5", ' ').replace(
        "\xf0\x9f\x91\x8d\x87\xba\xf0\x9f\x87\xb8", ' ').replace("\x9f", ' ').replace("\x91\x8d", ' ')
    text = text.replace("\xf0\x9f\x87\xba\xf0\x9f\x87\xb8", ' ').replace("\xf0", ' ').replace('\xf0x9f', '').replace(
        "\x9f\x91\x8d", ' ').replace("\x87\xba\x87\xb8", ' ')
    text = text.replace("\xe2\x80\x94", ' ').replace("\x9d\xa4", ' ').replace("\x96\x91", ' ').replace(
        "\xe1\x91\xac\xc9\x8c\xce\x90\xc8\xbb\xef\xbb\x89\xd4\xbc\xef\xbb\x89\xc5\xa0\xc5\xa0\xc2\xb8", ' ')
    text = text.replace("\xe2\x80\x99s", " ").replace("\xe2\x80\x98", ' ').replace("\xe2\x80\x99", ' ').replace(
        "\xe2\x80\x9c", " ").replace("\xe2\x80\x9d", " ")
    text = text.replace("\xe2\x82\xac", " ").replace("\xc2\xa3", " ").replace("\xc2\xa0", " ").replace("\xc2\xab",
                                                                                                       " ").replace(
        "\xf0\x9f\x94\xb4", " ").replace("\xf0\x9f\x87\xba\xf0\x9f\x87\xb8\xf0\x9f", "")
    return text


def nltk_tokenize(text):
    tokens = []
    pos_tokens = []
    entities = []
    features = []

    try:
        tokens = text.split()
        for word in tokens:
            if word.lower() not in stop_words and len(word) > 1:
                features.append(word)
    except:
        pass
    return [tokens, pos_tokens, entities, features]


'''Assumes its ok to remove user mentions and hashtags from tweet text (normalize_text), '''
'''since we extracted them already from the json object'''
def process_json_tweet(text, debug):
    features = []

    if len(text.strip()) == 0:
        return []
    text = normalize_text(text)
    # print text
    # nltk pre-processing: tokenize and pos-tag, try to extract entities
    try:
        [tokens, pos_tokens, entities, features] = nltk_tokenize(text)
    except:
        print("nltk tokenize+pos pb!")
    return features


'''Prepare features, where doc has terms separated by comma'''
def custom_tokenize_text(text):
    REGEX = re.compile(r",\s*")
    tokens = []
    for tok in REGEX.split(text):
        if "@" not in tok:
            tokens.append(tok.strip().lower())
    return tokens
