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
    nlp = spacy.load("es_core_news_md")
    doc = nlp(' '.join(interesting_tokens))
    interesting_tokens = [w.lemma_ for w in doc]

    return interesting_tokens


def mun_request(row, parameters):
    # utilizamos GoogleMaps API para determinar la ubicación correcta de los tweets extraídos por coordenadas
    if row['typeQuery'] == 'coordenadas':
        url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={row['latitude']},{row['longitude']}&key={credentials.GOOGLE_MAPS_KEY}"
        res = requests.get(url)
        elements = res.json()
        if  re.search("CDMX", elements['plus_code']['compound_code']):
            for i in elements['results']:
                if i['address_components'][0]['long_name'] in parameters["geo"].keys():
                    mun = i['address_components'][0]['long_name']
                    row['geoID'] = parameters["geo"][mun]["clave_alcaldia"]
                    row['geoName'] = mun
                    break
        else:
            row['geoID'] = 0
    return row

def whitout_acentos(text):
    return text.replace("á", "a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u")

def is_valid_tweet(tweet, topic, parameters):
    # convertimos el tweet a minúsculas
    tweet = tweet.lower()
    tweet = tweet.replace("á", "a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u")
    tweet = tweet.strip()
    
    # revisamos si el tweet contiene las palabras validas
    is_valid_contains = False
    for palabra in parameters["topics"][topic]["contains"]:
        if whitout_acentos(palabra) in tweet:
            is_valid_contains = is_valid_contains or True
        else:
            is_valid_contains = is_valid_contains or False

    # revisamos si el tweet contiene las palabras validas
    is_valid_not_contains = True
    for palabra in parameters["topics"][topic]["not_contains"]:
        if whitout_acentos(palabra) in tweet:
            is_valid_not_contains = is_valid_not_contains and False
        else:
            is_valid_not_contains = is_valid_not_contains and True

    return is_valid_contains and is_valid_not_contains

def log_df(df, year, month):
    # guardamos el registro de cuantos tweets quedaron por mes y año
    with open("./02_clean_tweets/01_log.txt", "a") as file:
        file.write(f'Se creo el archivo "tweets_{month}{year}" con {len(df)} registros: ')
        file.write(f'[pirotecnia: {len(df[df.topicQuery == "pirotecnia"])}, tránsito: {len(df[df.topicQuery == "tránsito"])}, incendio: {len(df[df.topicQuery == "incendio"])}]')
        file.write('\n')

def main():
    # abrimos el csv con los topics
    with open("./00_querys/topics.json") as file:
        parameters = json.load(file)
    # exploramos cada año
    for año in range(19,23):
        for mes in range (1,13):
            # abrimos un archivo de cada mes para hacer la limpieza
            if mes < 10:
                df = pd.read_csv(f'./01_tweets/tweets_0{mes}{año}.csv')
            else:
                df = pd.read_csv(f'./01_tweets/tweets_{mes}{año}.csv')
            # eliminamos registros repetidos
            df = df.drop_duplicates(['pubID'])
            # eliminar tweets que no cumplan con las palabras indicadas
            df["es_valido"] = df.apply(lambda x: is_valid_tweet(x["tweet"],x["topicQuery"],parameters), axis=1)
            df = df.drop(df[df["es_valido"] == False].index)
            df = df.drop(["es_valido"], axis=1)
            # obtener el municipio correcto de los registros extraídos por coordenadas
            df = df.apply(lambda x: mun_request(x,parameters), axis=1)
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
            if "tokens" in df.columns:
                df['tokens'] = df['tweet'].apply(tokens_tweet)
            else:
                df.insert(4, "tokens", df['tweet'].apply(tokens_tweet))
            # crear lista de hashtags
            if "hashtags" in df.columns:
                df["hashtags"] = df['tweet'].apply(lambda x: re.findall("\B#([\w-]+)", x))
            else:
                df.insert(5, "hashtags", df['tweet'].apply(lambda x: re.findall("\B#([\w-]+)", x)))
            # crear lista de menciones
            if "mentions" in df.columns:
                df["mentions"] = df['tweet'].apply(lambda x: re.findall("\B@([\w-]+)", x))
            else:
                df.insert(6, "mentions", df['tweet'].apply(lambda x: re.findall("\B@([\w-]+)", x)))
            # guardar dataset limpio
            if mes < 10:
                df.to_csv(f'./02_clean_tweets/clean_tweets_0{mes}{año}.csv', index=False)
                log_df(df, año, f'0{mes}')
            else:
                df.to_csv(f'./02_clean_tweets/clean_tweets_{mes}{año}.csv', index=False)
                log_df(df, año, mes)


if __name__ == '__main__':
    main()