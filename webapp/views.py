from django.shortcuts import render
from webapp.redis_interface import RedisInterface
from django.views.decorators.http import require_http_methods
from django.http import HttpResponse
from webapp.utils import fix_json_format, send_tweet_to_kafka
import json


@require_http_methods(["GET"])
def index(request):
    """
    This view if for show redis stats to user
    :param request:
    :return:
    """
    user = request.GET.get('user', 'oghab2')
    namad = request.GET.get('namad', 'شپنا')
    stats = {
        'post_in_6hours': RedisInterface.get_post_in_6hours(user=user),
        'last_posts': RedisInterface.get_last_posts(),
        'last_hashtags': RedisInterface.get_last_hashtags(),
        'post_count_for_namad': RedisInterface.get_post_count_for_namad(namad=namad),
        'post_in_1days': RedisInterface.get_post_in_1days(),
        'unique_hashtags_in_1hours': RedisInterface.get_unique_hashtags_in_1hour()
    }
    return render(request, 'webapp/index.html', {'stats': stats, 'user': user, 'namad': namad})


@require_http_methods(["POST", "PUT"])
def kafka_data(request):
    try:
        tweet = json.loads(request.body.decode('utf-8'))
        if not RedisInterface.duplicate_id(tweet['id']):
            tweet = fix_json_format(tweet)
            print('kafka:', tweet)
            RedisInterface.update_keys(tweet)
            send_tweet_to_kafka(tweet)
    except Exception as e:
        print(str(e))
    return HttpResponse('OK')
