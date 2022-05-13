import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
import re
import pandas as pd
from datetime import datetime

def tokens_tweet(tweet):
    pattern = r'''(?x)             # set flag to allow verbose regexps
              (?:[A-Z]\.)+         # abbreviations, e.g. U.S.A.
              | \w+(?:-\w+)*       # words with optional internal hyphens
              | \$?\d+(?:\.\d+)?%? # currency and percentages, e.g. $12.40, 82%
              | [][.,;"'?():-_`]   # these are separate tokens; includes ], [
    '''
    stop_words = stopwords.words('spanish')
    # Convertir todo el texto en minúsculas
    tweet = tweet.lower()
    # Remover menciones y links
    tweet = re.sub("@[A-Za-z0-9_]+","", tweet)
    tweet = re.sub(r"http\S+", "", tweet)
    tweet = re.sub(r"www.\S+", "", tweet)
    # Tokenización
    tokens_tweet = nltk.regexp_tokenize(tweet, pattern)
    tokens_tweet = [token for token in tokens_tweet if len(token) > 1]
    # Remover los stopwords
    interesting_tokens = [w for w in tokens_tweet if not w in stop_words]

    return interesting_tokens


def main():
    # Crear los DataFrames de cada tabla de la base de datos
    df_main = pd.DataFrame()
    df_data = pd.DataFrame()
    df_author = pd.DataFrame()
    df_date = pd.DataFrame()
    df_mun = pd.DataFrame({
        'munID': [2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17],
        'munName': ['Azcapotzalco', 
                    'Coyoacán', 
                    'Coajimalpa de Morelos', 
                    'Gustavo A. Madero', 
                    'Iztacalco',
                    'Iztapalapa',
                    'La Magdalena Contreras',
                    'Milpa Alta',
                    'Álvaro Obregón', 
                    'Tláhuac',
                    'Tlalpan',
                    'Xochimilco',
                    'Benito Juárez',
                    'Cuauhtémoc',
                    'Miguel Hidalgo',
                    'Venustiano Carranza'],
        'geoPointLong': [19.4853286147, 19.3266672536, 19.3246343001, 19.5040652077, 19.3969118970, 19.3491663204, 
                         19.2689765031, 19.1394565999, 19.3361755620, 19.2769983772, 19.1983396763, 19.2451450458,
                         19.3806424162, 19.4313734294, 19.4280623649, 19.4304954545],
        'geoPointLat': [-99.1821069423, -99.1503763525, -99.3107285253, -99.1158642087, -99.0943297970, -99.0567989642,
                        -99.2684129061, -99.0510954218, -99.2468197120, -99.0028216137, -99.2062207957, -99.0903636045,
                        -99.1611346584, -99.1490557562, -99.2045669144, -99.0931057959]
    })
    # Leer los archivos 
    df_tweets = pd.read_csv('./data/extracted_tweets_0120.csv')
    # Eliminar posibles registros duplicados basadnose en el pubID de twitter
    df_tweets = df_tweets.drop_duplicates(subset=['pubID'])

    # Empiezo el llenado de las tablas
    # df_data
    df_data['pubID'] = df_tweets['pubID']
    df_data['tokens'] = df_tweets['tweet'].apply(tokens_tweet)
    # df_author
    df_author['authorID'] = df_tweets['authorID']
    df_author['authorName'] = df_tweets['authorName']
    df_author['authorUsername'] = df_tweets['authorUsername']
    df_author['authorCreatedAt'] = df_tweets['authorCreatedAt']
    df_author['authorVerified'] = df_tweets['authorVerified']
    df_author['followersCount'] = df_tweets['followersCount']
    df_author['followingCount'] = df_tweets['followingCount']
    #df_date
    df_date['dateID'] = df_tweets.apply(lambda x : 
                                        datetime.strptime(x['pubDate'], '%Y-%m-%d %H:%M:%S').strftime('%Y%m%d%H%M'), 
                                        axis=1)
    df_date['pubYear'] = df_tweets['pubYear']
    df_date['pubMonth'] = df_tweets['pubMonth']
    df_date['pubDay'] = df_tweets['pubDay']
    df_date['pubHour'] = df_tweets['pubHour']
    df_date['pubMinute'] = df_tweets['pubMinute']
    # main
    df_main['pubID'] = df_tweets['pubID']
    df_main['querySearch'] = df_tweets['querySearch']
    df_main['tweet'] = df_tweets['tweet']
    df_main['likeCount'] = df_tweets['likeCount']
    df_main['replyCount'] = df_tweets['replyCount']
    df_main['retweetCount'] = df_tweets['retweetCount']
    df_main['authorID'] = df_author['authorID']
    df_main['dateID'] = df_date['dateID']

    print(df_main)

    


if __name__ == '__main__':
    main()