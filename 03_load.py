import numpy as np
import pandas as pd

def main():
    df = pd.DataFrame()
    for año in range(19,23):
        for mes in range (1,13):
            if mes < 10:
                df_aux = pd.read_csv(f'./csv_revisados/tweets_0{mes}{año}_c.csv')
            else:
                df_aux = pd.read_csv(f'./csv_revisados/tweets_{mes}{año}_c.csv')
            
            df = pd.concat([df,df_aux], sort=False, ignore_index=True)

    main = df[['pubID', 'topicQuery', 'tweet', 'likeCount', 
                'replyCount', 'retweetCount', 'authorID', 'geoID']]
    data = df[['pubID', 'tokens', 'mentions', 'hashtags']]
    author = df[['authorID', 'authorName', 'authorUsername',
                'authorCreatedAt', 'authorVerified', 'followersCount',
                'followingCount']]
    date = df[['pubDate', 'pubYear', 'pubMonth', 'pubDay', 
                'pubHour', 'pubMinute']]
    geo = df[['geoID', 'geoName']]
    coordinates = df[['longitude', 'latitude']]

    idx = list(df.index)

    main.insert(7, 'dateID', list(map(lambda x: 'd'+str(x+1), idx)))
    date.insert(0, 'dateID', list(map(lambda x: 'd'+str(x+1), idx)))
    geo.insert(2, 'coordinateID', list(map(lambda x: 'c'+str(x+1), idx)))
    coordinates.insert(0, 'coordinateID', list(map(lambda x: 'c'+str(x+1), idx)))

    main.to_csv('./mysql/main.csv', index=False)
    data.to_csv('./mysql/data.csv', index=False)
    author.to_csv('./mysql/author.csv', index=False)
    date.to_csv('./mysql/date.csv', index=False)
    geo.to_csv('./mysql/geo.csv', index=False)
    coordinates.to_csv('./mysql/coordinates.csv', index=False)


if __name__ == '__main__':
    main()