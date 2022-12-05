import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
import re
import pandas as pd
import numpy as np

def tokens_tweet(tweet):
    pattern = r'''(?x)           # set flag to allow verbose regexps
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
    for año in range(22,23):
        for mes in range (1,2):
            # abrimos un archivo de cada mes para hacer la limpieza
            if mes < 10:
                df = pd.read_csv(f'./new_data/tweets_0{mes}{año}.csv')
            else:
                df = pd.read_csv(f'./new_data/tweets_{mes}{año}.csv')
            # eliminamos registros repetidos
            df = df.drop_duplicates(['pubID'])
            # ordenar por fecha
            df['pubDate'] = pd.to_datetime(df['pubDate'])
            df = df.sort_values(by='pubDate')
            # cambiaremos los tipos de datos bool a 0 y 1 para su uso en MySQL
            df['authorVerified'] = df['authorVerified'].apply(lambda x: 1 if x else 0)
            # eliminamos registros con posibles datos nulos.
            cabeceras = list(df.columns)
            cabeceras.remove('likeCount')
            cabeceras.remove('replyCount')
            for columna in cabeceras:
                df = df[df[columna].notna()]
            # guardar dataset limpio
            if mes < 10:
                df.to_csv(f'./data_clean/clean_tweets_0{mes}{año}.csv', index=False)
            else:
                df.to_csv(f'./data_clean/clean_tweets_{mes}{año}.csv', index=False)


if __name__ == '__main__':
    main()