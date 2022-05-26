def search_on_tweet_text(query, start):
    # query variable should be a tuple of the format: (tweet_text_user_query, lower_time_range, upper_time_range)
    # if lower_time_range is not provided; then the default time is: "0001-01-01T01:01:01"
    # if upper_time_range is not provided; then the default time is: "9999-01-01T01:01:01"
    size = 10
    body = {
        "from": start,
        "size": size,
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {
                            "tweet_text": query[0]
                        }
                    }
                ],
                "filter": {
                    "range": {
                        "tweet_time": {
                            "gte": query[1],
                            "lte": query[2]
                        }
                    }
                }
            }
        }
        ,
        'highlight': {'fields': {'body': {}}}
    }

    return body

def search_on_userid(query, start):
    # query variable should be a tuple of the format: (userid_user_query, lower_time_range, upper_time_range)
    # if lower_time_range is not provided; then the default time is: '0001-01-01T01:01:01'
    # if upper_time_range is not provided; then the default time is: '9999-01-01T01:01:01'
    # (Timestamps should be enclosed in SINGLE QUOTES)
    # (User Query should be enclosed in DOUBLE QUOTES)
    size = 10
    body = {
        "from": start,
        "size": size,
        "query": "SELECT userid, account_creation_date, MAX(follower_count) AS follower_count, "
                 "MAX(following_count) AS following_count, MIN(tweet_time) as first_tweet_time, "
                 "MAX(tweet_time) as last_tweet_time, count(tweetid) as total_tweets "
                 "FROM twitter_index "
                 "WHERE tweet_time > CAST("+ query[1]+" AS DATETIME) and "
                 "tweet_time < CAST("+ query[2]+" AS DATETIME) "
                 "GROUP BY userid, account_creation_date",
        "filter": {
            "term": {
                "userid": query[0]
            }
        }
        ,
        'highlight': {'fields': {'body': {}}}
    }

    return body

# returns the json. Python code to pick the userids that are to be displayed
def search_on_follower_count(query, start):
    # query variable should be a tuple of the format: (follower_count, following_count)
    # follower_count should be an integer value
    # following_count should be an integer value
    size = 10
    body = {
        "from": start,
        "size": size,
        "query": {
            "bool": {
                "must": [
                    {
                        "match_all": {}
                    }
                ],
                "filter": [
                    {
                        "range": {
                            "follower_count": {
                                "gte": query[0]
                            }
                        }
                    },
                    {
                        "range": {
                            "following_count": {
                                "gte": query[1]
                            }
                        }
                    }
                ]
            }
        }
        ,
        'highlight': {'fields': {'body': {}}}
    }

    return body

# returns the json. Python code to pick the userids that are to be displayed
def search_on_locations(query, start):
    # query variable should be a tuple of the format: (follower_count, following_count)
    # follower_count should be an integer value
    # following_count should be an integer value
    size = 10
    body = {
        "from": start,
        "size": size,
        "query": {
            "bool": {
                "must": [
                    {
                        "match_all": {}
                    }
                ],
                "filter": [
                    {
                        "range": {
                            "follower_count": {
                                "gte": query[0]
                            }
                        }
                    },
                    {
                        "range": {
                            "following_count": {
                                "gte": query[1]
                            }
                        }
                    }
                ]
            }
        }
        ,
        'highlight': {'fields': {'body': {}}}
    }

    return body