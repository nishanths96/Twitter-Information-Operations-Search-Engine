import sys

from django.http import HttpResponse
from django.shortcuts import render
from . import models
from elasticsearch import Elasticsearch, helpers
import csv

ERR_QUERY_NOT_FOUND = '<h1>Query not found</h1>'
ERR_IMG_NOT_AVAILABLE = 'The requested result can not be shown now'

# USER = open("elastic-settings.txt").read().split("\n")[1]
# PASSWORD = open("elastic-settings.txt").read().split("\n")[2]
# change name for index
ELASTIC_INDEX = 'twitter_index'

# open connection to Elastic
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


# Include the following if user authentication is on (i.e., XPack`   is installed and linked with Elastic)
# http_auth=(USER, PASSWORD),

# SOLR_BASE_URL = "http://localhost:{0}/solr/{1}/select?&q=".format(SOLR_PORT,COLLECTION_NAME)
def home(request):
    return render(request, 'seer/display_tweets.html')


def query(request):
    if request.method == 'POST':
        q = request.POST.get('q', None)
        start = request.POST.get('start', 0)
        if q != None and len(q) > 2:
            return search(request, q, start)
        else:
            if q == None:
                return render(request, 'seer/index.html', {'errormessage': None})
            else:
                errormessage = 'Please use larger queries'
                return render(request, 'seer/index.html', {'errormessage': errormessage})
    else:  # it's a get request, can come from two sources. if start=0, or start not in GET dictionary, someone is requesting the page
        # for the first time

        start = int(request.GET.get('start', 0))
        query = request.GET.get('q', None)
        if start == 0 or query == None:
            return render(request, 'seer/index.html')
        else:
            return search(request, query, start)


def search(request, query, start):
    print(query)
    size = 10
    body = {
        "from": start,
        "size": size,
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["tweet_text", "user_profile_description"],
                "operator": "or"
            }
        },
        'highlight': {'fields': {'body': {}}}
    }
    res = es.search(index=ELASTIC_INDEX, body=body)

    if not res.get('hits'):

        return render(request, 'seer/error.html',
                      {'errormessage': 'Your query returned zero results, please try another query'})


    else:
        print("search done")
        totalresultsNumFound = res['hits']['total']
        # hlresults=r.json()['highlighting']
        results = res['hits']['hits']
        print(res['hits']['hits'])
        SearchResults = []
        if len(results) > 0:
            for result in results:
                resultid = result['_id']
                f = models.SearchResult(resultid)  # calling the object class that is defined inside models.py

                f.content = result['_source']['body']

                # rawpath= result['_source']['file']['url']

                # removing local folder path
                f.url = result['_source']['url']

                f.title = result['_source']['title']
                # f.description = str(result['_source']['meta']['raw']['description'])
                f.description = ''
                if 'highlight' in result:
                    for desc in result['highlight']['body']:
                        f.description = f.description + desc + '\n'

                # f.description = " ".join(f.description).encode("utf-8")
                '''
                if len(result.get('category',[])) > 0:
                   f.category=result['category'][0].encode("utf-8") 
                '''
                # trying to use the location field to get the file name to display the image
                # f.filename= str(imageid)+'.png'
                SearchResults.append(f)

            return render(request, 'seer/htmlresult.html', {'results': SearchResults, 'q': query, \
                                                            'total': totalresultsNumFound, 'i': str(start + 1) \
                , 'j': str(len(results) + start)})
        else:
            return render(request, 'seer/tweet_text_results.html',
                          {'errormessage': 'Your search returned zero results, please try another query'})


def download_results(search_results, column_headers, filename):
    csv_file_path = filename
    try:
        with open(csv_file_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=column_headers)
            writer.writeheader()
            for data in search_results:
                writer.writerow({k:str(v).encode('utf8') for k,v in data.items()})
    except IOError:
        print("I/O error")

def search_on_tweet_text(request):
    start = 0
    size = 20
    download_flag = False
    if request.GET.get('download_results'):
        download_flag = True
        size = 9999

    if request.GET.get('page_number'):
        page_number = int(request.GET.get('page_number'))
        start = (page_number - 1) * size + 1
    else:
        page_number = 1

    if request.GET.get('query_text'):
        query_text = request.GET.get('query_text')
    else:
        return render(request, 'seer/tweet_text_results.html',
                      {'nouid': ''})

    if request.GET.get('lower_time_stamp'):
        lower_time = request.GET.get('lower_time_stamp')
        print(lower_time)
    else:
        lower_time = "0001-01-01T01:01:01"

    if request.GET.get('upper_time_stamp'):
        upper_time = request.GET.get('upper_time_stamp')
        print("UPPER_TIME", upper_time)
    else:
        upper_time = "9999-01-01T01:01:01"

    request_params = {'query_text': query_text, 'lower_time_stamp': lower_time, 'upper_time_stamp': upper_time,
                      'page_number': page_number, 'previous_page_number': page_number - 1,
                      'next_page_number': page_number + 1
                      }
    body = {
        "from": start,
        "size": size,
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "tweet_text": query_text
                        }
                    }
                ],
                "filter": {
                    "range": {
                        "tweet_time": {
                            "gte": lower_time,
                            "lte": upper_time
                        }
                    }
                }
            }
        },
        'highlight': {'fields': {'body': {}}}
    }

    res = es.search(index=ELASTIC_INDEX, body=body)

    totalresultsNumFound = res['hits']['total']
    # hlresults=r.json()['highlighting']
    results = res['hits']['hits']
    print(res['hits']['hits'])
    SearchResults = []

    if len(results) > 0:
        for result in results:
            resultid = result['_id']
            f = {}
            # f = models.SearchResult(resultid)  # calling the object class that is defined inside models.py
            f['userid'] = result['_source']['userid']
            f['tweetid'] = result['_source']['tweetid']
            f['tweet_time'] = result['_source']['tweet_time']
            f['tweet_text'] = result['_source']['tweet_text']
            f['user_mentions'] = result['_source']['user_mentions']
            f['is_retweet'] = result['_source']['is_retweet']
            f['user_display_name'] = result['_source']['user_display_name']
            f['user_reported_location'] = result['_source']['user_reported_location']
            f['tweet_language'] = result['_source']['tweet_language']
            f['urls'] = result['_source']['urls']
            f['hashtags'] = result['_source']['hashtags']
            f['retweet_userid'] = result['_source']['retweet_userid']
            f['retweet_tweetid'] = result['_source']['retweet_tweetid']
            f['like_count'] = int(result['_source']['like_count'])
            f['reply_count'] = int(result['_source']['reply_count'])
            f['quote_count'] = int(result['_source']['quote_count'])
            # # rawpath= result['_source']['file']['url']
            #
            # # removing local folder path
            # f.url = result['_source']['url']
            #
            # f.title = result['_source']['title']
            # # f.description = str(result['_source']['meta']['raw']['description'])
            # f.description = ''
            # if 'highlight' in result:
            #     for desc in result['highlight']['body']:
            #         f.description = f.description + desc + '\n'

            # f.description = " ".join(f.description).encode("utf-8")
            '''
            if len(result.get('category',[])) > 0:
               f.category=result['category'][0].encode("utf-8")
            '''
            # trying to use the location field to get the file name to display the image
            # f.filename= str(imageid)+'.png'
            SearchResults.append(f)
    else:
        return render(request, 'seer/tweet_text_results.html', {'errormessage': 'No Results Found'})

    if download_flag:
        column_headers = ['userid', 'tweetid', 'tweet_time', 'tweet_text', 'user_mentions', 'is_retweet',
                          'user_display_name',
                          'user_reported_location', 'tweet_language', 'tweet_language', 'urls', 'hashtags',
                          'retweet_userid', 'retweet_tweetid', 'like_count', 'reply_count', 'quote_count']
        filename = "tweet_results_"+query_text+".csv"
        download_results(SearchResults, column_headers, filename)
        return render(request, 'seer/tweet_text_results.html', {'errormessage': 'Results have been downloaded as a csv'})

    return render(request, 'seer/tweet_text_results.html',
                  {'tweet_text_search_results': SearchResults, 'get_params': request_params})


# def search_tweets_home(request):
#     return render(request, 'seer/search_tweets.html')
#
#
# def search_userid_home(request):
#     return render(request, 'seer/search_userids.html')


def search_on_userid(request):
    start = 0
    size = 20
    download_flag = False

    if request.GET.get('download_results'):
        download_flag = True
        size = 9999

    if request.GET.get('page_number'):
        start = int(request.GET.get('page_number'))
        start = start * size - size + 1

    if request.GET.get('query_userid'):
        query_text = request.GET.get('query_userid')
    else:
        return render(request, 'seer/userid_results.html',
                      {
                          'no_uid': ''})

    if request.GET.get('lower_time_stamp'):
        lower_time = request.GET.get('lower_time_stamp')
        print(lower_time)
    else:
        lower_time = "0001-01-01T01:01:01"

    if request.GET.get('upper_time_stamp'):
        upper_time = request.GET.get('upper_time_stamp')
        print("UPPER_TIME", upper_time)
    else:
        upper_time = "9999-01-01T01:01:01"

    body1 = {
        "query": "SELECT userid, user_display_name, user_reported_location, user_profile_description, account_creation_date, MAX(follower_count) AS follower_count, "
                 "MAX(following_count) AS following_count, MIN(tweet_time) as first_tweet_time, "
                 "MAX(tweet_time) as last_tweet_time, count(tweetid) as total_tweets "
                 "FROM twitter_index "
                 "WHERE (tweet_time > CAST(" + "'%s'" % lower_time + " AS DATETIME) and "
                                                                     "tweet_time < CAST(" + "'%s'" % upper_time + " AS DATETIME)) AND userid LIKE " +
                 "'%s'" % query_text +
                 "GROUP BY userid, user_display_name, user_reported_location, user_profile_description, account_creation_date"
    }

    body2 = {
        "from": start,
        "size": 9999,
        "query":
            {
                "bool": {
                    "must": [
                        {"match": {
                            "userid": query_text
                        }}
                    ],
                    "filter": {
                        "range": {
                            "tweet_time": {
                                "gte": lower_time,
                                "lte": upper_time
                            }
                        }
                    }
                }
            },
        'highlight': {'fields': {'body': {}}}
    }

    # 1st results are the aggregated output
    sql_client = es.sql
    res1 = sql_client.query(body=body1)
    if not len(res1['rows']) > 0:
        return render(request, 'seer/userid_results.html',
                      {
                          'errormessage': 'No results Found'})
    user_details = res1['rows'][0]

    f = {}
    if len(user_details) > 0:
        f['userid'] = user_details[0]
        f['user_display_name'] = user_details[1]
        f['user_reported_location'] = user_details[2]
        f['user_profile_description'] = user_details[3]
        f['account_creation_date'] = user_details[4]
        f['follower_count'] = int(user_details[5])
        f['following_count'] = int(user_details[6])
        f['first_tweet_time'] = user_details[7]
        f['last_tweet_time'] = user_details[8]
        f['total_tweets'] = int(user_details[9])

    # 2nd results are to get all the hashtags and the URLs
    if download_flag:
        all_hash_tags = []
        all_urls = []
        stop_flag = False
        scroll_size = 0
        scroll_id = ""
        search_results = []
        column_headers = ['userid', 'user_display_name', 'user_reported_location', 'user_profile_description',
                          'account_creation_date', 'follower_count', 'following_count', 'first_tweet_time',
                          'last_tweet_time', 'total_tweets', 'urls', 'hashtags']
        while (scroll_size > 0) or (not stop_flag):
            if not stop_flag:
                res2 = es.search(index=ELASTIC_INDEX, body=body2, scroll='2m')
                scroll_id = res2['_scroll_id']
                stop_flag = True
            else:
                res2 = es.scroll(scroll_id = scroll_id, scroll = '2m')
                scroll_id = res2['_scroll_id']

            scroll_size = len(res2['hits']['hits'])
            results2 = res2['hits']['hits']
            for result in results2:
                for hashtag in result['_source']['hashtags']:
                    if hashtag:
                        all_hash_tags.append(hashtag)
                for url in result['_source']['urls']:
                    if url:
                        all_urls.append(url)

        f['urls'] = list(set(all_urls))
        f['hashtags'] = list(set(all_hash_tags))
        print("URLS:", len(f['urls']), "HASHTAGS:", len(f['hashtags']))
        search_results.append(f)
        filepath = "./user_id_" + query_text + "_details.csv"
        print(len(f['urls']), len(f['hashtags']))
        download_results(search_results, column_headers, filepath)
        return render(request, 'seer/userid_results.html',
                      {'errormessage': 'Results have been downloaded as a csv'})

    all_hash_tags = []
    all_urls = []
    res2 = es.search(index=ELASTIC_INDEX, body=body2)

    results2 = res2['hits']['hits']
    for result in results2:
        for hashtag in result['_source']['hashtags']:
            if hashtag:
                all_hash_tags.append(hashtag)
        for url in result['_source']['urls']:
            if url:
                all_urls.append(url)

    f['urls'] = list(set(all_urls))
    f['hashtags'] = list(set(all_hash_tags))

    return render(request, 'seer/userid_results.html', {'uid_search_results': f})


def search_on_locations(request):
    start = 0
    size = 20
    if request.GET.get('page_number'):
        start = int(request.GET.get('page_number'))
        start = start * size - size + 1

    if request.GET.get('query_location'):
        query_location = request.GET.get('query_location')
        # query_location = "%" + query_location + "%"
    else:
        return render(request, 'seer/location_results.html',
                      {
                          'no_uid': ''})

    # query to return the top 500 locations - sorted based on the tweet count.
    body = {
        "size": 0,
        "query": {
            "regexp": {
                "user_reported_location": {
                    "value": ".*"+query_location+".*",
                    "flags": "ALL"
                }
            }
        },
        "aggs": {
            "total_user_count": {
                "terms": {
                    "field": "user_reported_location.keyword",
                    "size": 500,
                    "order": {
                        "tweet_count": "desc"
                    }
                },
                "aggs": {
                    "user_count": {
                        "cardinality": {
                            "field": "userid.keyword",
                            "precision_threshold": 100
                        }
                    },
                    "tweet_count": {
                        "cardinality": {
                            "field": "tweetid",
                            "precision_threshold": 100
                        }
                    }
                }
            }
        }
    }

    f = {}
    f_id = 0
    res = es.search(index=ELASTIC_INDEX, body=body)
    results_list = res['aggregations']['total_user_count']['buckets']
    if not len(results_list) > 0:
        return render(request, 'seer/location_results.html', {'errormessage': "No results were found for this location."})
    else:
        for result in results_list:
            t = {}
            t['user_reported_location'] = result['key']
            t['total_tweets'] = result['tweet_count']['value']
            t['total_users'] = result['user_count']['value']
            f[f_id] = t
            f_id = f_id + 1

    return render(request, 'seer/location_results.html', {'location_search_results': f})


def search_on_tweet_bursts(request):
    if request.GET.get('user_location'):
        user_location = request.GET.get('user_location')
        user_location = "%" + user_location + "%"
    else:
        user_location = "%%"

    if request.GET.get('tweet_count'):
        tweet_count = int(request.GET.get('tweet_count'))
    else:
        return render(request, 'seer/tweet_burst_results.html')
    if request.GET.get('lower_time_stamp'):
        lower_time = request.GET.get('lower_time_stamp')
    else:
        return render(request, 'seer/tweet_burst_results.html',
                      {'errormessage': 'Please provide the the time range in a proper manner'})

    if request.GET.get('upper_time_stamp'):
        upper_time = request.GET.get('upper_time_stamp')
    else:
        return render(request, 'seer/tweet_burst_results.html',
                      {'errormessage': 'Please provide the the time range in a proper manner'})

    body = {
        "query": "SELECT userid, user_display_name, count(userid) as total_tweets FROM twitter_index "
                 "WHERE user_reported_location LIKE " + "'%s'" % user_location + " AND  "
                                                                                 "(tweet_time > CAST(" + "'%s'" % lower_time + "  AS DATETIME) AND "
                                                                                                                               "tweet_time < CAST(" + "'%s'" % upper_time + "  AS DATETIME)) "
                                                                                                                                                                            "GROUP BY userid,user_display_name HAVING total_tweets > " + str(
            tweet_count)
    }

    f = {}
    f_id = 0
    # sql client
    sql_client = es.sql
    stop_flag = False
    res1 = {}

    while "cursor" in res1 or not stop_flag:
        if not stop_flag:
            res1 = sql_client.query(body=body)
            stop_flag = True
            if not len(res1['rows']) > 0:
                print("HERE")
                return render(request, 'seer/tweet_burst_results.html',
                              {'errormessage': 'No results Found'})
        else:
            res1 = sql_client.query(body={"cursor": res1['cursor']})

        results = None
        if len(res1['rows']) > 0:
            results = res1['rows']
            for result in results:
                t = {}
                t['userid'] = str(result[0])
                t['user_display_name'] = str(result[1])
                t['total_tweets'] = result[2]
                f[f_id] = t
                f_id = f_id + 1
    print(f)
    return render(request, 'seer/tweet_burst_results.html', {'tweet_burst_results': f})
