import tweepy
import json
import credentials


def get_client():
    client = tweepy.Client(credentials.BEARER_TOKEN)
    """client = tweepy.Client(consumer_key=credentials.CONSUMER_KEY,
                           consumer_secret=credentials.CONSUMER_SECRET,
                           access_token=credentials.ACCESS_TOKEN,
                           access_token_secret=credentials.ACCESS_TOKEN_SECRET
                           )"""
    return client


def search_tweets(query):
    client = get_client()
    tweets = client.search_recent_tweets(query=query,
                                         max_results=10
                                         )
    tweets_data = tweets.data
    results = []

    if len(tweets_data) > 0:
        for tweet in tweets_data:
            obj = {}
            obj['id'] = tweet.id
            obj['text'] = tweet.text
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
