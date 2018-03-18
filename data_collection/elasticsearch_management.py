from elasticsearch import Elasticsearch
import sys
class elasticsearch_management:

    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.elasticsearch = Elasticsearch([{'host': host, 'port': int(port)}])

    def set_index_and_doc_type(self, index, doctype):
        self.index = index
        self.doctype = doctype

    def create_mapping_and_index(self, index, doctype):
        self.index = index
        self.doctype = doctype
        mapping = '''
                    {"mappings": {"''' + str(doctype) + '''": {"properties": {"created_at": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "display_text_range": {"type": "long"}, "entities": {"properties": {"hashtags": {"properties": {"indices": {"type": "long"}, "text": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}, "media": {"properties": {"display_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "expanded_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "indices": {"type": "long"}, "media_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "media_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "sizes": {"properties": {"large": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "medium": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "small": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "thumb": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}}}, "type": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}, "urls": {"properties": {"display_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "expanded_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "indices": {"type": "long"}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}}}, "extended_entities": {"properties": {"media": {"properties": {"display_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "expanded_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "indices": {"type": "long"}, "media_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "media_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "sizes": {"properties": {"large": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "medium": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "small": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "thumb": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}}}, "type": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}}}, "favorite_count": {"type": "long"}, "favorited": {"type": "boolean"}, "filter_level": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "is_quote_status": {"type": "boolean"}, "lang": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "possibly_sensitive": {"type": "boolean"}, "quote_count": {"type": "long"}, "reply_count": {"type": "long"}, "retweet_count": {"type": "long"}, "retweeted": {"type": "boolean"}, "source": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "text": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "timestamp_ms": {"type": "date", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "truncated": {"type": "boolean"}, "user": {"properties": {"contributors_enabled": {"type": "boolean"}, "created_at": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "default_profile": {"type": "boolean"}, "default_profile_image": {"type": "boolean"}, "description": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "favourites_count": {"type": "long"}, "followers_count": {"type": "long"}, "friends_count": {"type": "long"}, "geo_enabled": {"type": "boolean"}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "is_translator": {"type": "boolean"}, "lang": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "listed_count": {"type": "long"}, "location": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_image_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_image_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_tile": {"type": "boolean"}, "profile_banner_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_image_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_image_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_link_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_sidebar_border_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_sidebar_fill_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_text_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_use_background_image": {"type": "boolean"}, "protected": {"type": "boolean"}, "screen_name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "statuses_count": {"type": "long"}, "time_zone": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "translator_type": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "utc_offset": {"type": "long"}, "verified": {"type": "boolean"}}}}}}}
                     '''
        # {"mappings": {"my_doc_type": {"properties": {"created_at": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "display_text_range": {"type": "long"}, "entities": {"properties": {"hashtags": {"properties": {"indices": {"type": "long"}, "text": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}, "media": {"properties": {"display_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "expanded_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "indices": {"type": "long"}, "media_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "media_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "sizes": {"properties": {"large": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "medium": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "small": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "thumb": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}}}, "type": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}, "urls": {"properties": {"display_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "expanded_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "indices": {"type": "long"}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}}}, "extended_entities": {"properties": {"media": {"properties": {"display_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "expanded_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "indices": {"type": "long"}, "media_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "media_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "sizes": {"properties": {"large": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "medium": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "small": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}, "thumb": {"properties": {"h": {"type": "long"}, "resize": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "w": {"type": "long"}}}}}, "type": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}}}}}, "favorite_count": {"type": "long"}, "favorited": {"type": "boolean"}, "filter_level": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "is_quote_status": {"type": "boolean"}, "lang": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "possibly_sensitive": {"type": "boolean"}, "quote_count": {"type": "long"}, "reply_count": {"type": "long"}, "retweet_count": {"type": "long"}, "retweeted": {"type": "boolean"}, "source": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "text": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "timestamp_ms": {"type": "date", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "truncated": {"type": "boolean"}, "user": {"properties": {"contributors_enabled": {"type": "boolean"}, "created_at": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "default_profile": {"type": "boolean"}, "default_profile_image": {"type": "boolean"}, "description": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "favourites_count": {"type": "long"}, "followers_count": {"type": "long"}, "friends_count": {"type": "long"}, "geo_enabled": {"type": "boolean"}, "id": {"type": "long"}, "id_str": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "is_translator": {"type": "boolean"}, "lang": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "listed_count": {"type": "long"}, "location": {"type": "geo_point", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_image_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_image_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_background_tile": {"type": "boolean"}, "profile_banner_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_image_url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_image_url_https": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_link_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_sidebar_border_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_sidebar_fill_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_text_color": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "profile_use_background_image": {"type": "boolean"}, "protected": {"type": "boolean"}, "screen_name": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "statuses_count": {"type": "long"}, "time_zone": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "translator_type": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "url": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}, "utc_offset": {"type": "long"}, "verified": {"type": "boolean"}}}}}}}
        self.elasticsearch.indices.create(index=index, body=mapping)
        print(self.elasticsearch.indices.get_mapping(index=index))

    def save_in_elasticsearch(self, json_data, id_document):
        try:
            self.elasticsearch.create(index= self.index, doc_type=self.doctype, id=int(id_document), body=json_data)
            #print(str(id_document) + " " + json_data)
            #print("ok in save_in_elasticsearch")
        except:
            print("Unexpected error in save_in_elasticsearch", sys.exc_info())
            pass

