import redis
from django.conf import settings
from datetime import datetime, timedelta

redis_client = redis.Redis(host=settings.REDIS_SETTINGS['host'], port=settings.REDIS_SETTINGS['port'])


class RedisInterface:

    @staticmethod
    def get_post_in_6hours(user):
        ans = 0
        for i in range(6):
            key = 'user${}${}'.format(user, (datetime.now() - timedelta(hours=i)).isoformat(timespec='hours'))
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
            key = 'hashtag${}${}'.format(namad, (datetime.now() - timedelta(hours=i)).isoformat(timespec='hours'))
            value = redis_client.get(key)
            if i == 0:
                _1hour = value
            if i < 6:
                _6hour += value
            _24hour += value

        return {1: _1hour, 6: _6hour, 24: _24hour}

    @staticmethod
    def update_keys(tweet):
        username = tweet.get('username', '')
        hashtags = tweet.get('hashtags', [])
        content = tweet.get('content', '')
        time = datetime.strptime(tweet.get('sendTime'), '%Y-%m-%dT%H:%M:%SZ')
        RedisInterface.update_user_day_hour_key(username, time)
        RedisInterface.update_day_hour_key(time)
        RedisInterface.update_count_hashtag_day_hour_key(hashtags, time)
        RedisInterface.update_last_posts_key(content, time)
        RedisInterface.update_last_hashtags_key(hashtags, time)

    @staticmethod
    def update_user_day_hour_key(username, time):
        key = 'user${}${}'.format(username, time.isoformat(timespec='hours'))
        old_value = redis_client.get(key) if redis_client.exists(key) else 0
        redis_client.set(key, old_value + 1, ex=7 * 24 * 60 * 60)  # expire after one week

    @staticmethod
    def update_day_hour_key(time):
        key = 'post${}'.format(time.isoformat(timespec='hours'))
        old_value = redis_client.get(key) if redis_client.exists(key) else 0
        redis_client.set(key, old_value + 1, ex=7 * 24 * 60 * 60)  # expire after one week

    @staticmethod
    def update_count_hashtag_day_hour_key(hashtags, time):
        cnt = 0
        for hashtag in hashtags:
            key = 'hashtag${}${}'.format(hashtag, time.isoformat(timespec='hours'))
            old_value = redis_client.get(key) if redis_client.exists(key) else 0
            redis_client.set(key, old_value + 1, ex=7 * 24 * 60 * 60)  # expire after one week

            if old_value == 0:
                cnt += 1
        cnt_key = 'cnt$hashtags${}'.format(time.isoformat(timespec='hours'))
        old_cnt_value = redis_client.get(cnt_key) if redis_client.exists(cnt_key) else 0
        redis_client.set(cnt_key, old_cnt_value + cnt, ex=7 * 24 * 60 * 60)  # expire after one week

    @staticmethod
    def update_last_posts_key(content, time):
        key = 'last$posts'
        if redis_client.exists(key):
            old_value = redis_client.get(key)
            old_value.append((content, time))
            old_value.sort(key=lambda x: x[1], reverse=True)
            redis_client.set(key, old_value[:100], ex=7 * 24 * 60 * 60)  # expire after one week
        else:
            redis_client.set(key, [(content, time)], ex=7 * 24 * 60 * 60)  # expire after one week

    @staticmethod
    def update_last_hashtags_key(hashtags, time):
        key = 'last$hashtags'
        if redis_client.exists(key):
            old_value = redis_client.get(key)
            old_value.extend([(h, time) for h in hashtags])
            old_value.sort(key=lambda x: x[1], reverse=True)
            redis_client.set(key, old_value[:1000], ex=7 * 24 * 60 * 60)  # expire after one week
        else:
            redis_client.set(key, [(h, time) for h in hashtags], ex=7 * 24 * 60 * 60)  # expire after one week
