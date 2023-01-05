import numpy as np
import pandas as pd
import emoji

def deEmojify(text):
    return emoji.replace_emoji(text, "")

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
                'replyCount', 'retweetCount', 'authorID']]
    data = df[['pubID', 'tokens', 'mentions', 'hashtags']]
    author = df[['authorID', 'authorName', 'authorUsername',
                'authorCreatedAt', 'authorVerified', 'followersCount',
                'followingCount']]
    date = df[['pubDate', 'pubYear', 'pubMonth', 'pubDay', 
                'pubHour', 'pubMinute']]
    coordinates = df[['geoID', 'geoName', 'longitude', 'latitude']]

    idx = list(df.index)

    main['tweet'] = main['tweet'].apply(lambda x: x.replace("\n", " "))
    main['tweet'] = main['tweet'].apply(deEmojify)
    main.insert(7, 'dateID', list(map(lambda x: 'd'+str(x+1), idx)))
    main.insert(8, 'coordinateID', list(map(lambda x: 'c'+str(x+1), idx)))
    author['authorName'] = author['authorName'].apply(deEmojify)
    author['authorUsername'] = author['authorUsername'].apply(deEmojify)
    date.insert(0, 'dateID', list(map(lambda x: 'd'+str(x+1), idx)))
    coordinates.insert(0, 'coordinateID', list(map(lambda x: 'c'+str(x+1), idx)))

    main.to_csv('./mysql/main.csv', index=False)
    data.to_csv('./mysql/data.csv', index=False)
    author.to_csv('./mysql/author.csv', index=False)
    date.to_csv('./mysql/date.csv', index=False)
    coordinates.to_csv('./mysql/coordinates.csv', index=False)


if __name__ == '__main__':
    main()