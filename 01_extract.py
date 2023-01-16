import snscrape.modules.twitter as sntwitter
import datetime
import calendar
import pandas as pd
import json

class database:

    def __init__(self):
        self.columns =  ['pubID',
                        'topicQuery',
                        'typeQuery',
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
                        'geoID',
                        'geoName',
                        'longitude',
                        'latitude']
        self.df = pd.DataFrame(columns=self.columns)

    def append_rows(self, rows):
        self.df = pd.concat([self.df, pd.DataFrame(rows, columns=self.columns)], sort=False)

    def save_df(self, year, month):
        self.df.sort_values('pubID', inplace=True)
        self.df.to_csv(f'./01_tweets/tweets_{month}{year}.csv', index=False)
        self.log_df(year, month)

    def log_df(self, year, month):
        with open("./01_tweets/01_log.txt", "a") as file:
            file.write(f'Se creo el archivo "tweets_{month}{year}" con {len(self.df)} registros: ')
            file.write(f'[pirotecnia: {len(self.df[self.df.topicQuery == "pirotecnia"])}, tránsito: {len(self.df[self.df.topicQuery == "tránsito"])}, incendio: {len(self.df[self.df.topicQuery == "incendio"])}]')
            file.write('\n')

def search_tweets_snscrape(query, topic, type_query, name_mun, geoid, latitud, longitud):
    results = []
    for tweet in list(sntwitter.TwitterSearchScraper(query).get_items()):
        tweet_row = []
        tweet_row.append(tweet.id)
        tweet_row.append(topic)
        tweet_row.append(type_query)
        tweet_row.append(tweet.content.replace("\n"," "))
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
        tweet_row.append(geoid)
        tweet_row.append(name_mun)
        if tweet.coordinates != None:
            tweet_row.append(tweet.coordinates.longitude)
            tweet_row.append(tweet.coordinates.latitude)
        else:
            tweet_row.append(longitud)
            tweet_row.append(latitud)
        results.append(tweet_row)

    return results

def main():
    # el usuario introduce el año que quiere extraer
    year = input('Define the year (YYYY): ')

    # abrimos el csv con los topics
    with open("./00_querys/topics.json") as file:
        parameters = json.load(file)

    # for para cada mes del año
    for count in range(1,13):
        # creamos nuestro dataframe para guardar los resultados
        my_df = database()
        # creamos las fechas para el intervalo de tiempo de la query
        if count < 10:
            start_date = f'{year}-0{count}-01'
        else:
            start_date = f'{year}-{count}-01'
        
        if count+1 < 10:
            end_date= f'{year}-0{count+1}-01'
        elif count == 12:
            end_date= f'{int(year)+1}-01-01'
        else:
            end_date= f'{year}-{count+1}-01'

        start_date_time = datetime.datetime.strptime(start_date, '%Y-%m-%d') + \
                    datetime.timedelta(hours=6)
        start_date_time = calendar.timegm(start_date_time.utctimetuple())
        end_date_time = datetime.datetime.strptime(end_date, '%Y-%m-%d') + \
                        datetime.timedelta(hours=6)
        end_date_time = calendar.timegm(end_date_time.utctimetuple())

        # creación de las querys
        for topic, meta in parameters["topics"].items():
            # palabras clave
            keywords = f'({" OR ".join(meta["sinonimos"])})'.replace("'",'"')
            
            for alc, meta_alc in parameters["geo"].items():
                # creamos las palabras clave de alcaldía e incluimos conjuntos de palabras que no queremos
                alcaldia = f'({" OR ".join(meta_alc["sinonimos"])})'.replace("'",'"')
                geocode = f'geocode:{meta_alc["latitud"]},{meta_alc["longitud"]},{meta_alc["radio"]}'
                exceptions_alcaldia = f'-("calle {alc}" OR "av {alc}" OR "avenida {alc}" OR "col {alc}" OR "colonia {alc}" OR "carretera {alc}" OR "ciudad {alc}")'
                alcaldia_keywords = " ".join([alcaldia, exceptions_alcaldia])
                geocode_keywords = " ".join([geocode, exceptions_alcaldia])
                # creamos los dos tipos de query (por palabras clave y por geocode)
                query = f'{keywords} {alcaldia_keywords} since:{start_date_time} until:{end_date_time} -is:retweet lang:es'
                query_geocode = f'{keywords} {geocode_keywords} since:{start_date_time} until:{end_date_time} -is:retweet lang:es'
                # extraemos los tweets con la query de palabras clave
                results = search_tweets_snscrape(query, topic, 'palabras clave', alc, meta_alc["clave_alcaldia"], meta_alc["latitud"], meta_alc["longitud"])
                my_df.append_rows(results)
                # extraemos los tweets con la query de geocode
                results = search_tweets_snscrape(query_geocode, topic, 'coordenadas', alc, meta_alc["clave_alcaldia"], meta_alc["latitud"], meta_alc["longitud"])
                my_df.append_rows(results)

        my_df.save_df(start_date[2:4], start_date[5:7])
        print(f'mes {count} concluido')


if __name__ == '__main__':
    main()