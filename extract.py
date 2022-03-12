import csv
import datetime
import os
import sys
import tweepy
import credentials


def get_client():
    client = tweepy.Client(bearer_token=credentials.BEARER_TOKEN,
                           consumer_key=credentials.CONSUMER_KEY,
                           consumer_secret=credentials.CONSUMER_SECRET,
                           access_token=credentials.ACCESS_TOKEN,
                           access_token_secret=credentials.ACCESS_TOKEN_SECRET
                           )
    return client


def fill_dataset(data, file_name):
    file = f'./data/{file_name}'
    if not os.path.exists(file):
        with open(file, 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile, delimiter=',')
            csv_writer.writerow(['pubID',
                                 'querySearch',
                                 'tweet',
                                 'likeCount',
                                 'replyCount',
                                 'retweetCount',
                                 'authorID',
                                 'authorName',
                                 'authorUsername',
                                 'authorCreatedAt',
                                 'authorVerified',
                                 'followersCount',
                                 'followingCount',
                                 'pubDate',
                                 'pubYear',
                                 'pubMonth',
                                 'pubDay',
                                 'pubHour',
                                 'pubMinute',
                                 'extDate',
                                 'geoID',
                                 'geoCountry',
                                 'geoFullname',
                                 'geoName',
                                 'geoType',
                                 'geoBbox'])

    with open(file, 'a', newline='') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',')
        csv_writer.writerows(data)


def search_tweets(query, start_time, end_time, file_name):
    client = get_client()
    tweets = client.search_recent_tweets(query=query,
                                         max_results=100,
                                         tweet_fields=[
                                             'created_at', 'geo', 'public_metrics', 'author_id'],
                                         user_fields=[
                                             'created_at', 'public_metrics', 'verified'],
                                         place_fields=[
                                             'country', 'geo', 'name', 'place_type'],
                                         expansions=[
                                             'geo.place_id', 'author_id'],
                                         start_time=start_time +
                                         datetime.timedelta(hours=6),
                                         end_time=end_time +
                                         datetime.timedelta(hours=6)
                                         )
    ext_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    tweets_data = tweets.data

    if tweets_data:
        tweets_includes = tweets.includes
        if tweets_includes:
            user_objects = {}
            place_objects = {}
            for expansion in tweets_includes.keys():
                if expansion == 'users':
                    user_objects = {
                        user.id: {
                            'authorName': user.name,
                            'authorUsername': user.username,
                            'authorCreatedAt': user.created_at,
                            'authorVerified': user.verified,
                            'followersCount': user.public_metrics['followers_count'],
                            'followingCount': user.public_metrics['following_count']
                        } for user in tweets_includes['users']}
                if expansion == 'places':
                    place_objects = {
                        place.id: {
                            'geoCountry': place.country,
                            'geoFullname': place.full_name,
                            'geoName': place.name,
                            'geoType': place.place_type,
                            'geoBbox': place.geo['bbox']
                        } for place in tweets_includes['places']}

        results = []
        for tweet in tweets_data:
            tweet_row = []
            tweet_row.append(tweet.id)
            tweet_row.append(query)
            tweet_row.append(tweet.text)
            tweet_row.append(tweet.public_metrics['like_count'])
            tweet_row.append(tweet.public_metrics['reply_count'])
            tweet_row.append(tweet.public_metrics['retweet_count'])
            tweet_row.append(tweet.author_id)
            tweet_row.append(user_objects[tweet.author_id]['authorName'])
            tweet_row.append(user_objects[tweet.author_id]['authorUsername'])
            tweet_row.append((user_objects[tweet.author_id]['authorCreatedAt']
                              - datetime.timedelta(hours=6)).strftime('%Y-%m-%d %H:%M:%S'))
            tweet_row.append(user_objects[tweet.author_id]['authorVerified'])
            tweet_row.append(user_objects[tweet.author_id]['followersCount'])
            tweet_row.append(user_objects[tweet.author_id]['followingCount'])
            tweet_date = tweet.created_at-datetime.timedelta(hours=6)
            tweet_row.append(tweet_date.strftime('%Y-%m-%d %H:%M:%S'))
            tweet_row.append(tweet_date.strftime('%Y'))
            tweet_row.append(tweet_date.strftime('%m'))
            tweet_row.append(tweet_date.strftime('%d'))
            tweet_row.append(tweet_date.strftime('%H'))
            tweet_row.append(tweet_date.strftime('%M'))
            tweet_row.append(ext_date)
            try:
                tweet_row.append(tweet.geo['place_id'])
                tweet_row.append(
                    place_objects[tweet.geo['place_id']]['geoCountry'])
                tweet_row.append(
                    place_objects[tweet.geo['place_id']]['geoFullname'])
                tweet_row.append(
                    place_objects[tweet.geo['place_id']]['geoName'])
                tweet_row.append(
                    place_objects[tweet.geo['place_id']]['geoType'])
                tweet_row.append(
                    place_objects[tweet.geo['place_id']]['geoBbox'])
            except:
                for i in range(6):
                    tweet_row.append(None)

            results.append(tweet_row)

        fill_dataset(results, file_name)
    else:
        return 0

    return len(tweets_data)


def main(day, month, query_file, file_name):
    try:
        with open(f'./data/{query_file}', 'r', encoding='utf-8') as file:
            for query in file:
                tweets_found = 0
                query_clean = query.replace("\n","")
                for i in range(23):
                    tweets_found += search_tweets(query_clean, datetime.datetime(2022, month, day, hour=i),
                                                  datetime.datetime(2022, month, day, hour=i+1), file_name)

                tweets_found += search_tweets(query_clean, datetime.datetime(2022, month, day, hour=23),
                                              datetime.datetime(2022, month, day+1), file_name)
                
                print(f'The query "{query_clean}" on date {datetime.datetime(2022, month, day).strftime("%Y-%m-%d")} returned {tweets_found} results.')

    except FileNotFoundError:
        print("The file whit the querys doesn't exist.")


if __name__ == '__main__':
    try:
        main(int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], sys.argv[4])
    except IndexError:
        print("Introduce the day, month and file_name of the search in console's args.")
