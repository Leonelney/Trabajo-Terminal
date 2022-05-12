import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
import re
import pandas as pd

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
    df_geo = pd.DataFrame()
    df_mun = pd.DataFrame()
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
    df_date['dateID'] = df_tweets['']
    df_date['pubYear'] = df_tweets['pubYear']
    df_date['pubMonth'] = df_tweets['pubMonth']
    df_date['pubDay'] = df_tweets['pubDay']
    df_date['pubHour'] = df_tweets['pubHour']
    df_date['pubMinute'] = df_tweets['pubMinute']
    #df_geo
    # main
    df_main['pubID'] = df_tweets['pubID']
    df_main['querySearch'] = df_tweets['querySearch']
    df_main['tweet'] = df_tweets['tweet']

    print(df_author)

    


if __name__ == '__main__':
    main()