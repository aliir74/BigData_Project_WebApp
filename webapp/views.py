from django.shortcuts import render
from webapp.redis_interface import RedisInterface
from django.views.decorators.http import require_http_methods
from django.http import HttpResponse
import json



def index(request):
    """
    This view if for show redis stats to user
    :param request:
    :return:
    """
    stats = {
        'post_in_6hours': RedisInterface.get_post_in_6hours(user='test'),
        'last_posts': RedisInterface.get_last_posts(),
        'last_hashtags': RedisInterface.get_last_hashtags(),
        'post_count_for_namad': RedisInterface.get_post_count_for_namad(namad='شکاپا'),
        'post_in_1days': RedisInterface.get_post_in_1days(),
        'unique_hashtags_in_1hours': RedisInterface.get_unique_hashtags_in_1hour()
    }
    return render(request, 'webapp/index.html', {'stats': stats})

@require_http_methods(["POST", "PUT"])
def kafka_data(request):
    #print(request.read())
    try:
        print(request.body.decode('utf-8'))
        print('Kafka:', json.loads(request.body.decode('utf-8')))
    except Exception as e:
        print(str(e))
    return HttpResponse('OK')

