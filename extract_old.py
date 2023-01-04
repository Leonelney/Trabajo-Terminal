import csv
import datetime
import calendar
import os
import sys
import tweepy
import snscrape.modules.twitter as sntwitter
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
        with open(file, 'w', newline='', encoding='utf-8') as csvfile:
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
                                'geoBbox',
                                'geoCoordinates'])

    with open(file, 'a', newline='', encoding='utf-8') as csvfile:
        csv_writer = csv.writer(csvfile, delimiter=',')
        csv_writer.writerows(data)


def search_tweets_tweepy(query, start_time, end_time, file_name):
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
            try:
                tweet_row.append(tweet.geo['coordinates'].coordinates)
            except:
                tweet_row.append(None)

            results.append(tweet_row)

        fill_dataset(results, file_name)
    else:
        return 0

    return len(tweets_data)


def search_tweets_snscrape(query, start, end, file_name):
    start_date_time = datetime.datetime.strptime(start, '%Y-%m-%d') + datetime.timedelta(hours=6)
    end_date_time = datetime.datetime.strptime(end, '%Y-%m-%d') + datetime.timedelta(hours=6)

    full_query = query + \
        f' since_time:{calendar.timegm(start_date_time.utctimetuple())} until_time:{calendar.timegm(end_date_time.utctimetuple())}'

    results = []
    tweets_found = 0
    ext_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for tweets_found, tweet in enumerate(sntwitter.TwitterSearchScraper(full_query).get_items()):
        tweet_row = []
        tweet_row.append(tweet.id)
        tweet_row.append(query)
        tweet_row.append(tweet.content)
        tweet_row.append(tweet.likeCount)
        tweet_row.append(tweet.replyCount)
        tweet_row.append(tweet.retweetCount)
        tweet_row.append(tweet.user.id)
        tweet_row.append(tweet.user.displayname)
        tweet_row.append(tweet.user.username)
        tweet_row.append((tweet.user.created - datetime.timedelta(hours=6)).strftime('%Y-%m-%d %H:%M:%S'))
        tweet_row.append(tweet.user.verified)
        tweet_row.append(tweet.user.followersCount)
        tweet_row.append(tweet.user.friendsCount)
        tweet_date = tweet.date - datetime.timedelta(hours=6)
        tweet_row.append(tweet_date.strftime('%Y-%m-%d %H:%M:%S'))
        tweet_row.append(tweet_date.strftime('%Y'))
        tweet_row.append(tweet_date.strftime('%m'))
        tweet_row.append(tweet_date.strftime('%d'))
        tweet_row.append(tweet_date.strftime('%H'))
        tweet_row.append(tweet_date.strftime('%M'))
        tweet_row.append(ext_date)
        tweet_row.append(None)
        if tweet.place != None:
            tweet_row.append(tweet.place.country)
            tweet_row.append(tweet.place.fullName)
            tweet_row.append(tweet.place.name)
            tweet_row.append(tweet.place.type)
        else:
            for i in range(4):
                tweet_row.append(None)
        tweet_row.append(None)
        if tweet.coordinates != None:
            coordinate = []
            coordinate.append(tweet.coordinates.longitude)
            coordinate.append(tweet.coordinates.latitude)
            tweet_row.append(coordinate)
        else:
            tweet_row.append(tweet.coordinates)

        results.append(tweet_row)
    
    fill_dataset(results, file_name)
    return tweets_found



def main(query_file, file_name):
    os.system('clear')
    try:
        with open(f'./querys/{query_file}', 'r', encoding = 'utf-8') as file:
            while True:
                method=input(
                    'Which method will you use? Tweepy or Snscrape (t/s)?: ')
                if method == 't':
                    os.system('clear')
                    day=int(input('- Define the search day: '))
                    month=int(input('- Define the search month: '))
                    os.system('clear')
                    
                    for query in file:
                        tweets_found=0
                        query_clean=query.replace("\n", "")
                        for i in range(23):
                            tweets_found += search_tweets_tweepy(query_clean, datetime.datetime(2022, month, day, hour=i),
                                                        datetime.datetime(2022, month, day, hour=i+1), file_name)

                        try:
                            tweets_found += search_tweets_tweepy(query_clean, datetime.datetime(2022, month, day, hour=23),
                                                    datetime.datetime(2022, month, day+1), file_name)
                        except ValueError:
                            tweets_found += search_tweets_tweepy(query_clean, datetime.datetime(2022, month, day, hour=23),
                                                    datetime.datetime(2022, month+1, 1), file_name)
                        
                        print("*"*90)
                        print(f'The query "{query_clean}" \n\non date {datetime.datetime(2022, month, day).strftime("%Y-%m-%d")} \n\nreturned {tweets_found} results.\n')
                    break
                elif method == 's':
                    os.system('clear')
                    start_date = input('Define the start date (YYYY-mm-dd):')
                    end_date = input('Define the end date (YYYY-mm-dd):')
                    os.system('clear')

                    for query in file:
                        query_clean = query.replace("\n","")
                        tweets_found = search_tweets_snscrape(query_clean, start_date, end_date, file_name)

                        print("*"*90)
                        print(f'The query "{query_clean}" \n\nfrom {start_date} to {end_date} \n\nreturned {tweets_found} results.\n')
                    break

    except FileNotFoundError:
        print("The file whit the querys doesn't exist.")
    

if __name__ == '__main__':
    try:
        main(sys.argv[1], sys.argv[2])
    except IndexError:
        print("Introduce the query_file and file_name of the search in console's args.")
