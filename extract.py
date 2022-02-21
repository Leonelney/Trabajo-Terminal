import tweepy
import json


def get_credentials():
    try:
        with open("./credentials.txt", "r") as f:
            keys = f.read().split("\n")
            return keys
    except:
        print("An error occurred with the credentials file.")


def get_client():
    keys = get_credentials()
    client = tweepy.Client(consumer_key=keys[0],
                           consumer_secret=keys[1],
                           access_token=keys[2],
                           access_token_secret=keys[3]
                           )
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
