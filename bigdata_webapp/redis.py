import redis
from django.conf import settings

redis_client = redis.Redis(host=settings.REDIS_SETTINGS['host'], port=settings.REDIS_SETTINGS['port'])
