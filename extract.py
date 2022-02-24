import tweepy
import json
import datetime
import credentials


def get_client():
    client = tweepy.Client(bearer_token=credentials.BEARER_TOKEN,
                           consumer_key=credentials.CONSUMER_KEY,
                           consumer_secret=credentials.CONSUMER_SECRET,
                           access_token=credentials.ACCESS_TOKEN,
                           access_token_secret=credentials.ACCESS_TOKEN_SECRET
                           )
    return client


def search_tweets(query):
    client = get_client()
    tweets = client.search_recent_tweets(query=query,
                                         max_results=40,
                                         tweet_fields=["created_at", "geo", "lang"],
                                         expansions=["geo.place_id"]
                                         )
    tweets_data = tweets.data
    tweets_includes = tweets.includes
    if tweets_includes:
        dict_places = {places.id:places.full_name for places in tweets_includes['places']}  
    results = []

    if len(tweets_data) > 0:
        for tweet in tweets_data:
            obj = {}
            obj['data'] = tweet['data']
            obj['data']['created_at'] = (tweet.created_at - datetime.timedelta(hours=6)).strftime('%Y-%m-%d %H:%M:%S')
            if tweet.geo:
                obj['data']['geo_name'] = dict_places[tweet.geo['place_id']]
            results.append(obj)

    else:
        print("No tweets found")
        return

    with open("./data/tweets_extract.json", "w", encoding="utf-8") as f:
        f.write(json.dumps(results))

    print("Tweets found")


def main():
    query = 'pirotecnia'
    search_tweets(query)


if __name__ == "__main__":
    main()
