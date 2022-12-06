import snscrape.modules.twitter as sntwitter
import datetime
import calendar
import pandas as pd

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
        self.df.to_csv(f'./new_data/tweets_{month}{year}.csv', index=False)
        self.log_df(year, month)

    def log_df(self, year, month):
        with open("./new_data/log.txt", "a") as file:
            file.write(f'Se creo el archivo "tweets_{month}{year}" con {len(self.df)} registros: ')
            file.write(f'[pirotecnia: {len(self.df[self.df.topicQuery == "pirotecnia"])}, tránsito: {len(self.df[self.df.topicQuery == "tránsito"])}, incendio: {len(self.df[self.df.topicQuery == "incendio"])}]')
            file.write('\n')

def save_register(tweet, query, typeQuery, geoid, coordenadas, name_mun):
    tweet_row = []
    tweet_row.append(tweet.id)
    tweet_row.append(query.split(' ')[0][1:])
    tweet_row.append(typeQuery)
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
    tweet_row.append(geoid)
    tweet_row.append(name_mun)
    if tweet.coordinates != None:
        tweet_row.append(tweet.coordinates.longitude)
        tweet_row.append(tweet.coordinates.latitude)
    else:
        tweet_row.append(coordenadas.split(',')[1])
        tweet_row.append(coordenadas.split(',')[0])
    
    return tweet_row


def search_tweets_snscrape(query, start_date, end_date):
    geo_municipios = [
        (2, '19.4853286147,-99.1821069423', '5km', 'Azcapotzalco', 'azcapotzalco'),
        (3, '19.3266672536,-99.1503763525', '6km', 'Coyoacán', 'coyoacan'),
        (4, '19.3246343001,-99.3107285253', '10km', 'Cuajimalpa de Morelos', '("cuajimalpa de morelos" OR cuajimalpa)'),
        (5, '19.5040652077,-99.1158642087', '9km', 'Gustavo A. Madero', '("gustavo a. madero" OR gam)'),
        (6, '19.3969118970,-99.0943297970', '5km', 'Iztacalco', 'iztacalco'),
        (7, '19.3491663204,-99.0567989642', '9km', 'Iztapalapa' , 'iztapalapa'),
        (8, '19.2689765031,-99.2684129061', '10km', 'La Magdalena Contreras', '"magdalena contreras"'),
        (9, '19.1394565999,-99.0510954218', '11km', 'Milpa Alta', '"milpa alta"'),
        (10, '19.3361755620,-99.2468197120', '9km', 'Álvaro Obregón' , '"alvaro obregon"'),
        (11, '19.2769983772,-99.0028216137', '7km', 'Tláhuac', 'tlahuac'),
        (12, '19.1983396763,-99.2062207957', '11km', 'Tlalpan', 'tlalpan'),
        (13, '19.2451450458,-99.0903636045', '7km', 'Xochimilco', 'xochimilco'),
        (14, '19.3806424162,-99.1611346584', '3km', 'Benito Juárez', '"benito juárez"'),
        (15, '19.4313734294,-99.1490557562', '4km', 'Cuauhtémoc', 'cuauhtemoc'),
        (16, '19.4280623649,-99.2045669144', '5km', 'Miguel Hidalgo', '"miguel hidalgo"'),
        (17, '19.4304954545,-99.0931057959', '5km', 'Venustiano Carranza', '"venustiano carranza"')
    ]

    meta_search = '-is:retweet lang:es'
    delimitadores = '(reportar OR denuncia OR denunciar OR contaminación OR ilegal OR ruido OR estrés OR contingencia OR precaución OR prohibido OR detengan OR alto)'
    start_date_time = datetime.datetime.strptime(start_date, '%Y-%m-%d') + \
                    datetime.timedelta(hours=6)
    start_date_time = calendar.timegm(start_date_time.utctimetuple())
    end_date_time = datetime.datetime.strptime(end_date, '%Y-%m-%d') + \
                    datetime.timedelta(hours=6)
    end_date_time = calendar.timegm(end_date_time.utctimetuple())

    results = []
    for geoid, coordenadas, radio, name_mun, mun_query in geo_municipios:
        if query == "(pirotecnia OR cohete OR 'fuegos artificiales')":
            new_query = f'{query} geocode:{coordenadas},{radio} since:{start_date_time} until:{end_date_time} {meta_search}'
        else:
            new_query = f'{query} {delimitadores} geocode:{coordenadas},{radio} since:{start_date_time} until:{end_date_time} {meta_search}'
            
        for tweet in list(sntwitter.TwitterSearchScraper(new_query).get_items()):
            results.append(save_register(tweet, query, 'coordenadas', geoid, coordenadas, name_mun))
        
        if query == "(pirotecnia OR cohete OR 'fuegos artificiales')":
            new_query = f'{query} {mun_query} since:{start_date_time} until:{end_date_time} {meta_search}'
        else:
            new_query = f'{query} {delimitadores} {mun_query} since:{start_date_time} until:{end_date_time} {meta_search}'
            
        for tweet in list(sntwitter.TwitterSearchScraper(new_query).get_items()):
            results.append(save_register(tweet, query, 'palabras clave', geoid, coordenadas, name_mun))

    return results

def main():
    topics = [
        "(pirotecnia OR cohete OR 'fuegos artificiales')", 
        "(tránsito OR tráfico)",
        "(incendio OR humo OR fuego)"
    ]

    year = input('Define the year (YYYY): ')

    for count in range(1,13):
        my_df = database()
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

        for query in topics:
            results = search_tweets_snscrape(query, start_date, end_date)
            my_df.append_rows(results)

        my_df.save_df(start_date[2:4], start_date[5:7])


if __name__ == '__main__':
    main()