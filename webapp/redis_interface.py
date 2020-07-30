import redis
from django.conf import settings
from datetime import datetime, timedelta

redis_client = redis.Redis(host=settings.REDIS_SETTINGS['host'], port=settings.REDIS_SETTINGS['port'])


class RedisInterface:

    @staticmethod
    def get_post_in_6hours(user):
        ans = 0
        for i in range(6):
            key = '{}${}'.format(user, (datetime.now()-timedelta(hours=i)).isoformat(timespec='hours'))
            ans += redis_client.get(key)
        return ans

    @staticmethod
    def get_post_in_1days():
        ans = 0
        for i in range(24):
            key = 'post${}'.format((datetime.now() - timedelta(hours=i)).isoformat(timespec='hours'))
            ans += redis_client.get(key)
        return ans

    @staticmethod
    def get_unique_hashtags_in_1hour():
        key = 'cnt$hashtags${}'.format((datetime.now()).isoformat(timespec='hours'))
        ans = redis_client.get(key)
        return ans

    @staticmethod
    def get_last_hashtags():
        key = 'last$hashtags'
        ans = redis_client.get(key)
        return ans

    @staticmethod
    def get_last_posts():
        key = 'last$posts'
        ans = redis_client.get(key)
        return ans

    @staticmethod
    def get_post_count_for_namad(namad):
        _24hour = 0
        _6hour = 0
        _1hour = 0
        for i in range(24):
            key = '{}${}'.format(namad, (datetime.now() - timedelta(hours=i)).isoformat(timespec='hours'))
            value = redis_client.get(key)
            if i == 0:
                _1hour = value
            if i < 6:
                _6hour += value
            _24hour += value

        return {1: _1hour, 6: _6hour, 24: _24hour}

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
