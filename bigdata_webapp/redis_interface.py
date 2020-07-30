import redis
from django.conf import settings

redis_client = redis.Redis(host=settings.REDIS_SETTINGS['host'], port=settings.REDIS_SETTINGS['port'])


class RedisInterface:

    def __init__(self):
        pass

    @staticmethod
    def update_keys(post):
        RedisInterface.update_user_day_hour_key()
        RedisInterface.update_day_hour_key()
        RedisInterface.update_hashtag_day_hour_key()
        RedisInterface.update_count_hashtag_day_hour_key()
        RedisInterface.update_last_posts_key()
        RedisInterface.update_last_hashtags_key()

    @staticmethod
    def update_user_day_hour_key():
        pass

    @staticmethod
    def update_day_hour_key():
        pass

    @staticmethod
    def update_hashtag_day_hour_key():
        pass

    @staticmethod
    def update_count_hashtag_day_hour_key():
        pass

    @staticmethod
    def update_last_posts_key():
        pass

    @staticmethod
    def update_last_hashtags_key():
        pass

    @staticmethod
    def update_namad_day_hour_key():
        pass
