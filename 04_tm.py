import mysql.connector
import numpy as np
import pandas as pd
from collections import defaultdict
from gensim import corpora
from gensim.models import Phrases
import ast

def main():
    df = pd.DataFrame()
    for año in range(19,23):
        for mes in range (1,13):
            if mes < 10:
                df_aux = pd.read_csv(f'./data_clean/clean_tweets_0{mes}{año}.csv')
            else:
                df_aux = pd.read_csv(f'./data_clean/clean_tweets_{mes}{año}.csv')
            
            df = pd.concat([df,df_aux], sort=False, ignore_index=True)

    texts = list(df['tokens'])
    texts = [ast.literal_eval(tokens) for tokens in texts]

    # remove words that appear only once
    frequency = defaultdict(int)
    for text in texts:
        for token in text:
            frequency[token] += 1
    texts = [[token for token in text if frequency[token] > 1] for text in texts]
    # Add bigrams to docs (only ones that appear 20 times or more).
    bigram = Phrases(texts, min_count=20)
    for idx in range(len(texts)):
        for token in bigram[texts[idx]]:
            if '_' in token:
                # Token is a bigram, add to document.
                texts[idx].append(token)
    
    # Create the dictionary
    dictionary = corpora.Dictionary(texts)
    # Filter out words that occur less than X documents, 
    # or more than X% of the documents.
    dictionary.filter_extremes(no_below=50, no_above=0.5)
    # Create the corpus.  This is a Term Frequency 
    # or Bag of Words representation.
    corpus = [dictionary.doc2bow(text) for text in texts]
    # print tokens and len of documents
    print(f'Number of unique tokens: {len(dictionary)}')
    print(f'Number of documents: {len(corpus)}')
    

if __name__ == '__main__':
    main()