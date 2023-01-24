import numpy as np
import pandas as pd
import ast
import emoji

def deEmojify(text):
    return emoji.replace_emoji(text, "")

def main():
    # abrimos todos los datasets y los combinamos 
    df = pd.DataFrame()
    for año in range(19,23):
        for mes in range (1,13):
            if mes < 10:
                df_aux = pd.read_csv(f'./02_clean_tweets/clean_tweets_0{mes}{año}.csv', lineterminator='\n')
            else:
                df_aux = pd.read_csv(f'./02_clean_tweets/clean_tweets_{mes}{año}.csv', lineterminator='\n')
            
            df = pd.concat([df,df_aux], sort=False, ignore_index=True)

    # creamos cada tabla de la base de datos en dataframes
    main = df[['pubID', 'topicQuery', 'tweet', 'likeCount', 
                'replyCount', 'retweetCount', 'authorID']]
    data = df[['pubID', 'tokens', 'mentions', 'hashtags']]
    author = df[['authorID', 'authorName', 'authorUsername',
                'authorCreatedAt', 'authorVerified', 'followersCount',
                'followingCount']]
    date = df[['pubDate', 'pubYear', 'pubMonth', 'pubDay', 
                'pubHour', 'pubMinute']]
    coordinates = df[['geoID', 'geoName', 'longitude', 'latitude']]

    # agregamos id a algunas tablas y complementamos otras
    idx = list(df.index)
    main['tweet'] = main['tweet'].apply(lambda x: x.replace("\n", " "))
    main['tweet'] = main['tweet'].apply(deEmojify)
    main.insert(7, 'dateID', list(map(lambda x: 'd'+str(x+1), idx)))
    main.insert(8, 'coordinateID', list(map(lambda x: 'c'+str(x+1), idx)))
    data["tokens"] = data["tokens"].apply(lambda x: ','.join(ast.literal_eval(x)))
    data["mentions"] = data["mentions"].apply(lambda x: ','.join(ast.literal_eval(x)))
    data["hashtags"] = data["hashtags"].apply(lambda x: ','.join(ast.literal_eval(x)))
    author = author.drop_duplicates(subset="authorID")
    author['authorName'] = author['authorName'].apply(deEmojify)
    author['authorUsername'] = author['authorUsername'].apply(deEmojify)
    date.insert(0, 'dateID', list(map(lambda x: 'd'+str(x+1), idx)))
    coordinates.insert(0, 'coordinateID', list(map(lambda x: 'c'+str(x+1), idx)))

    # obtenemos las tablas en formato CSV
    main.to_csv('./03_tables/dim_hechos.csv', index=False)
    data.to_csv('./03_tables/dim_metadata.csv', index=False)
    author.to_csv('./03_tables/dim_usuario.csv', index=False)
    date.to_csv('./03_tables/dim_tiempo.csv', index=False)
    coordinates.to_csv('./03_tables/dim_ubicación.csv', index=False)


if __name__ == '__main__':
    main()