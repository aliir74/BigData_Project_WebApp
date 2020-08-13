import redis
from django.conf import settings
from datetime import datetime, timedelta
import json

redis_client = redis.Redis(host=settings.REDIS_SETTINGS['host'], port=settings.REDIS_SETTINGS['port'])


def remove_duplicates(l):
    hash_map = {}
    duplicate_list = [False for i in l]
    for i in range(len(l)):
        key = l[i][0]
        if key in hash_map.keys():
            duplicate_list[i] = True
        else:
            hash_map[key] = 1
    for i in range(len(l) - 1, -1, -1):
        if duplicate_list[i]:
            del l[i]


class RedisInterface:

    @staticmethod
    def duplicate_id(id):
        if redis_client.exists(id):
            return True
        redis_client.set(id, 'id')
        if redis_client.exists('duplicate_cnt'):
            old_value = int(redis_client.get('duplicate_cnt').decode())
        else:
            old_value = 0
        redis_client.set('duplicate_id', old_value+1)
        return False

    @staticmethod
    def get_post_in_6hours(user):
        ans = 0
        for i in range(6):
            key = 'user${}${}'.format(user, (datetime.now() - timedelta(hours=i)).isoformat(timespec='hours'))
            ans += int(redis_client.get(key).decode()) if redis_client.exists(key) else 0
        return ans

    @staticmethod
    def get_post_in_1days():
        ans = 0
        for i in range(24):
            key = 'post${}'.format((datetime.now() - timedelta(hours=i)).isoformat(timespec='hours'))
            ans += int(redis_client.get(key).decode()) if redis_client.exists(key) else 0
        return ans

    @staticmethod
    def get_unique_hashtags_in_1hour():
        key = 'cnt$hashtags${}'.format((datetime.now()).isoformat(timespec='hours'))
        ans = int(redis_client.get(key).decode()) if redis_client.exists(key) else 0
        return ans

    @staticmethod
    def get_last_hashtags():
        key = 'last$hashtags'
        ans = [x[0] for x in json.loads(redis_client.get(key).decode())['key']] if redis_client.exists(key) else []
        return ans

    @staticmethod
    def get_last_posts():
        key = 'last$posts'
        ans = [x[0] for x in json.loads(redis_client.get(key).decode())['key']] if redis_client.exists(key) else []
        return ans

    @staticmethod
    def get_post_count_for_namad(namad):
        _24hour = 0
        _6hour = 0
        _1hour = 0
        for i in range(24):
            key = 'hashtag${}${}'.format(namad, (datetime.now() - timedelta(hours=i)).isoformat(timespec='hours'))
            value = int(redis_client.get(key).decode()) if redis_client.exists(key) else 0
            if i == 0:
                _1hour = value
            if i < 6:
                _6hour += value
            _24hour += value

        return {1: _1hour, 6: _6hour, 24: _24hour}

    @staticmethod
    def update_keys(tweet):
        username = tweet.get('senderUsername', '')
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
        old_value = int(redis_client.get(key).decode()) if redis_client.exists(key) else 0
        redis_client.set(key, old_value + 1, ex=7 * 24 * 60 * 60)  # expire after one week

    @staticmethod
    def update_day_hour_key(time):
        key = 'post${}'.format(time.isoformat(timespec='hours'))
        old_value = int(redis_client.get(key).decode()) if redis_client.exists(key) else 0
        redis_client.set(key, old_value + 1, ex=7 * 24 * 60 * 60)  # expire after one week

    @staticmethod
    def update_count_hashtag_day_hour_key(hashtags, time):
        cnt = 0
        for hashtag in hashtags:
            key = 'hashtag${}${}'.format(hashtag, time.isoformat(timespec='hours'))
            old_value = int(redis_client.get(key).decode()) if redis_client.exists(key) else 0
            redis_client.set(key, old_value + 1, ex=7 * 24 * 60 * 60)  # expire after one week

            if old_value == 0:
                cnt += 1
        cnt_key = 'cnt$hashtags${}'.format(time.isoformat(timespec='hours'))
        old_cnt_value = int(redis_client.get(cnt_key).decode()) if redis_client.exists(cnt_key) else 0
        redis_client.set(cnt_key, old_cnt_value + cnt, ex=7 * 24 * 60 * 60)  # expire after one week

    @staticmethod
    def update_last_posts_key(content, time):
        key = 'last$posts'
        if redis_client.exists(key):
            old_value = json.loads(redis_client.get(key).decode())
            old_value['key'].append([content, time.isoformat()])
            old_value['key'].sort(key=lambda x: x[1], reverse=True)
            old_value['key'] = old_value['key'][:100]
            redis_client.set(key, json.dumps(old_value), ex=7 * 24 * 60 * 60)  # expire after one week
        else:
            redis_client.set(key, json.dumps({
                'key': [(content, time.isoformat())]
            }), ex=7 * 24 * 60 * 60)  # expire after one week

    @staticmethod
    def update_last_hashtags_key(hashtags, time):
        key = 'last$hashtags'
        if redis_client.exists(key):
            old_value = json.loads(redis_client.get(key).decode())
            old_value['key'].extend([[h, time.isoformat()] for h in hashtags])
            old_value['key'].sort(key=lambda x: x[1], reverse=True)
            remove_duplicates(old_value['key'])
            old_value['key'] = old_value['key'][:1000]
            redis_client.set(key, json.dumps(old_value), ex=7 * 24 * 60 * 60)  # expire after one week
        else:
            redis_client.set(key, json.dumps({
                'key': [[h, time.isoformat()] for h in hashtags]
            }), ex=7 * 24 * 60 * 60)  # expire after one week
