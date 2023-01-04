import pandas as pd
import numpy as np

def main():
    for año in range(19,23):
        for mes in range (1,13):
            if mes < 10:
                df_lim = pd.read_csv(f'./datos_limpios/clean_tweets_0{mes}{año}.csv')
                df_rev = pd.read_csv(f'./csv_revisados/tweets_0{mes}{año}_c.csv')
            else:
                df_lim = pd.read_csv(f'./datos_limpios/clean_tweets_{mes}{año}.csv')
                df_rev = pd.read_csv(f'./csv_revisados/tweets_{mes}{año}_c.csv')
            
            df_lim['validacion'] = df_lim['tweet'].apply(lambda x: True if x in list(df_rev['tweet']) else False)
            filtro = df_lim['validacion'] == True
            df_lim = df_lim[filtro]
            df_lim.drop(['validacion'], axis=1)
            
            if mes < 10:
                df_lim.to_csv(f'./csv_revisados/tweets_0{mes}{año}_c.csv', index=False)
            else:
                df_lim.to_csv(f'./csv_revisados/tweets_{mes}{año}_c.csv', index=False)
    

if __name__ == '__main__':
    main()