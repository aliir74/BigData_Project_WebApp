def fix_json_format(tweet):
    tweet['hashtags'] = tweet['Hashtags'][1:-1].replace("'", "").split(',')
    # print(tweet['hashtags'])
    tweet['id'] = int(tweet['id'])
    tweet['sendTimeUNIX'] = int(tweet['sendTimeUNIX'].replace('\n', ''))
    del tweet['Hashtags']
    keywords = ['بورس', 'اقتصاد', 'تحریم', 'دولت', 'دلار', 'طلا', 'کرونا', 'شاخص بورس', 'تورم', 'دانشگاه', 'سقوط',
                'رشد']
    tweet['keywords'] = []
    split_content = tweet['content'].split()
    for keyword in keywords:
        if keyword in split_content:
            tweet['keywords'].append(keyword)
    return tweet
