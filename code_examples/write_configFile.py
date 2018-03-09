import configparser

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config['DEFAULT'] = {}
    config['DEFAULT']['index'] = 'my_index'
    config['DEFAULT']['doc_type'] = 'my_doc_type'
    config['DEFAULT']['host'] = 'localhost'
    config['DEFAULT']['port'] = '9200'
    config['DEFAULT']['image_directory'] = 'img'
    '''config['bitbucket.org'] = {}
    config['bitbucket.org']['User'] = 'hg'
    config['topsecret.server.com'] = {}
    topsecret = config['topsecret.server.com']
    topsecret['Port'] = '50022'     # mutates the parser
    config['DEFAULT']['ForwardX11'] = 'yes' '''
    with open('config.ini', 'w') as configfile:
        config.write(configfile)
