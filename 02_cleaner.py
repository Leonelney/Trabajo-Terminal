import nltk
nltk.download('stopwords')
from nltk.corpus import stopwords
import spacy
from autocorrect import Speller
import requests
import json
import re
import datetime
import pandas as pd
import numpy as np
import credentials

def tokens_tweet(tweet):
    pattern = r'''(?x)           # set flag to allow verbose regexps
            (?:[A-Z]\.)+         # abbreviations, e.g. U.S.A.
            | \w+(?:-\w+)*       # words with optional internal hyphens
            | \$?\d+(?:\.\d+)?%? # currency and percentages, e.g. $12.40, 82%
            | [][.,;"'?():-_`]   # these are separate tokens; includes ], [
    '''
    # obtenemos la lista de stopwords
    stop_words = stopwords.words('spanish')
    # Convertir todo el texto en minúsculas
    tweet = tweet.lower()
    # Remover menciones, hashtags, links y saltos de linea
    tweet = re.sub("@[A-Za-z0-9_]+","", tweet)
    tweet = re.sub("#\S+","", tweet)
    tweet = re.sub(r"http\S+", "", tweet)
    tweet = re.sub(r"www.\S+", "", tweet)
    tweet = re.sub(r'\n', '', tweet)
    # Tokenización
    tokens_tweet = nltk.regexp_tokenize(tweet, pattern)
    tokens_tweet = [token for token in tokens_tweet if len(token) > 1]
    # Remover los stopwords
    interesting_tokens = [w for w in tokens_tweet if not w in stop_words]
    # corregir palabras
    spell = Speller(lang='es')
    interesting_tokens = [spell(w) for w in interesting_tokens]
    # lematización
    nlp = spacy.load("es_dep_news_trf")
    doc = nlp(' '.join(interesting_tokens))
    interesting_tokens = [w.lemma_ for w in doc]

    return interesting_tokens


def mun_request(row):
    geo_mun = {
        'Azcapotzalco': 2,
        'Coyoacán': 3,
        'Cuajimalpa de Morelos': 4,
        'Gustavo A. Madero': 5,
        'Iztacalco': 6,
        'Iztapalapa': 7,
        'La Magdalena Contreras': 8,
        'Milpa Alta': 9,
        'Álvaro Obregón': 10,
        'Tláhuac': 11,
        'Tlalpan': 12,
        'Xochimilco': 13,
        'Benito Juárez': 14,
        'Cuauhtémoc': 15, 
        'Miguel Hidalgo': 16, 
        'Venustiano Carranza': 17
    }
    if row['typeQuery'] == 'coordenadas':
        url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={row['latitude']},{row['longitude']}&key={credentials.GOOGLE_MAPS_KEY}"
        res = requests.get(url)
        elements = res.json()
        mun = ''
        if  re.search("CDMX", elements['plus_code']['compound_code']):
            for i in elements['results']:
                if i['address_components'][0]['long_name'] in geo_mun.keys():
                    mun = i['address_components'][0]['long_name']
                    row['geoID'] = geo_mun[mun]
                    row['geoName'] = mun
                    break
        else:
            row['geoID'] = 0
    return row


def main():
    
    for año in range(20,23):
        print(año)
        for mes in range (1,13):
            print(f'\t{mes}')
            # abrimos un archivo de cada mes para hacer la limpieza
            if mes < 10:
                df = pd.read_csv(f'./new_data/tweets_0{mes}{año}.csv')
            else:
                df = pd.read_csv(f'./new_data/tweets_{mes}{año}.csv')
            # eliminamos registros repetidos
            df = df.drop_duplicates(['pubID'])
            # obtener el municipio correcto de los registros extraídos por coordenadas
            df = df.apply(mun_request, axis=1)
            # eliminar registros que están fuera de la CDMX
            df = df.drop(df[df['geoID'] == 0].index)
            # cambiaremos los tipos de datos bool a 0 y 1 para su uso en MySQL
            df['authorVerified'] = df['authorVerified'].apply(lambda x: 1 if x else 0)
            # eliminamos registros con posibles datos nulos de los campos importantes.
            cabeceras = list(df.columns)
            cabeceras.remove('likeCount')
            cabeceras.remove('replyCount')
            cabeceras.remove('retweetCount')
            cabeceras.remove('followersCount')
            cabeceras.remove('followingCount')
            for columna in cabeceras:
                df = df[df[columna].notna()]
            # modificamos las fechas de creación del tweet de acuerdo a los horarios de verano
            if año == 19:
                df['pubDate'] = df['pubDate'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=1) \
                                                    if  datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S') > \
                                                        datetime.datetime(2019,4,7,2) and \
                                                        datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S') < \
                                                        datetime.datetime(2019,11,3,2) \
                                                    else x)
            elif año == 20:
                df['pubDate'] = df['pubDate'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=1) \
                                                    if  datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S') > \
                                                        datetime.datetime(2020,4,5,2) and \
                                                        datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S') < \
                                                        datetime.datetime(2020,10,25,2) \
                                                    else x)
            elif año == 21:
                df['pubDate'] = df['pubDate'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=1) \
                                                    if  datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S') > \
                                                        datetime.datetime(2021,4,7,2) and \
                                                        datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S') < \
                                                        datetime.datetime(2021,10,31,2) \
                                                    else x)
            elif año == 22:
                df['pubDate'] = df['pubDate'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(hours=1) \
                                                    if  datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S') > \
                                                        datetime.datetime(2022,4,3,2) and \
                                                        datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S') < \
                                                        datetime.datetime(2022,10,30,2) \
                                                    else x)
            df['pubYear'] = df['pubDate'].apply(lambda x: datetime.datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').strftime('%Y'))
            df['pubMonth'] = df['pubDate'].apply(lambda x: datetime.datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').strftime('%m'))
            df['pubDay'] = df['pubDate'].apply(lambda x: datetime.datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').strftime('%d'))
            df['pubHour'] = df['pubDate'].apply(lambda x: datetime.datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').strftime('%H'))
            df['pubMinute'] = df['pubDate'].apply(lambda x: datetime.datetime.strptime(str(x), '%Y-%m-%d %H:%M:%S').strftime('%M'))
            # ordenar por fecha
            df['pubDate'] = pd.to_datetime(df['pubDate'])
            df = df.sort_values(by='pubDate')
            # creamos los tokens de cada tweet
            df.insert(4, "tokens", df['tweet'].apply(tokens_tweet))
            # crear lista de hashtags
            df.insert(5, "hashtags", df['tweet'].apply(lambda x: re.findall("\B#([\w-]+)", x)))
            # crear lista de menciones
            df.insert(6, "mentions", df['tweet'].apply(lambda x: re.findall("\B@([\w-]+)", x)))
            # guardar dataset limpio
            if mes < 10:
                df.to_csv(f'./data_clean/clean_tweets_0{mes}{año}.csv', index=False)
            else:
                df.to_csv(f'./data_clean/clean_tweets_{mes}{año}.csv', index=False)


if __name__ == '__main__':
    main()