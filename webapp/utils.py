import requests, json
from django.conf import settings


def fix_json_format(tweet):
    tweet['hashtags'] = tweet['Hashtags'][1:-1].replace("'", "").split(',')
    # print(tweet['hashtags'])
    tweet['id'] = int(tweet['id'])
    tweet['sendTimeUNIX'] = int(tweet['sendTimeUNIX'].replace('\n', ''))
    tweet['username'] = tweet['senderUsername']
    del tweet['Hashtags']
    keywords = ['بورس', 'اقتصاد', 'تحریم', 'دولت', 'دلار', 'طلا', 'کرونا', 'شاخص بورس', 'تورم', 'دانشگاه', 'سقوط',
                'رشد']
    tweet['keywords'] = []
    split_content = tweet['content'].split()
    for keyword in keywords:
        if keyword in split_content:
            tweet['keywords'].append(keyword)
    return tweet


def send_tweet_to_kafka(tweet):
    post_body = {
        "records": [
            {
                "value": tweet
            }
        ]
    }
    res = requests.post(settings.ELASTIC_KAFKA_URL, data=json.dumps(post_body),
                        headers={'Content-Type': 'application/vnd.kafka.json.v2+json'})
    print('kafka elastic', res.status_code)
