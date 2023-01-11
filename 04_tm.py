import ast
import numpy as np
import pandas as pd
from collections import defaultdict

from gensim import corpora
from gensim.models import Phrases
from gensim.models import CoherenceModel
from gensim.models.ldamodel import LdaModel

import pyLDAvis
import pyLDAvis.gensim_models as gensimvis

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import seaborn as sns
import logging


def compute_coherence_values(dictionary, corpus, texts, 
                            cohere, limit, start, step,
                            chunksize, passes, iterations):
    
    coherence_values = []

    for num_topics in range(start, limit, step):
        model = LdaModel(corpus=corpus, 
                        id2word=dictionary, 
                        num_topics=num_topics,
                        chunksize=chunksize,
                        alpha='auto',
                        eta='auto',
                        iterations=iterations,
                        passes=passes,
                        eval_every=None,
                        random_state=42,)
        coherencemodel = CoherenceModel(model=model, 
                                        texts=texts, 
                                        dictionary=dictionary, 
                                        coherence=cohere)
        coherence_values.append(coherencemodel.get_coherence())

    return coherence_values


def get_topic_coherence_score(dictionary, corpus, texts, chunksize, passes, iterations):
    limit=30
    start=2
    step=2

    coherence_values = compute_coherence_values(dictionary=dictionary, 
                                            corpus=corpus, 
                                            texts=texts, 
                                            cohere='c_v', # 'u_mass', 'c_v', 'c_uci', 'c_npmi'
                                            start=start, 
                                            limit=limit, 
                                            step=step,
                                            chunksize=chunksize, 
                                            passes=passes, 
                                            iterations=iterations)

    return (coherence_values.index(max(coherence_values))+1)*2
    

def get_ngrams(texts, n):
    if n < 2:
        return texts
    else:
        res = []
        for tokens in texts:
            aux = []
            for i in range(2,n+1):
                aux = aux + ['_'.join(tokens[j:j+i]) for j in range(len(tokens)-(i-1))]
            aux = tokens + aux
            res.append(aux)
        return res

def remove_one_appear(texts):
    frequency = defaultdict(int)
    for text in texts:
        for token in text:
            frequency[token] += 1
    return [[token for token in text if frequency[token] > 1] for text in texts]

def get_model(dictionary, corpus, texts, titulo):
    # Training the Model
    chunksize = len(texts)
    passes = 10
    iterations = 200
    eval_every = None
    temp = dictionary[0]
    
    NUM_TOPICS = get_topic_coherence_score(dictionary, corpus, texts, chunksize, passes, iterations)
    model = LdaModel(
        corpus=corpus,
        id2word=dictionary.id2token,
        chunksize=chunksize,
        alpha='auto',
        eta='auto',
        iterations=iterations,
        num_topics=NUM_TOPICS,
        passes=passes,
        eval_every=eval_every
    )

    # feed the LDA model into the pyLDAvis instance
    lda_viz = gensimvis.prepare(model, corpus, dictionary, sort_topics=True)
    pyLDAvis.save_html(lda_viz, f'./resultados_tm/lda_{titulo}.html')

def topic_modeling(df, topics, titulo):
    texts = list(df['tokens'])
    texts = [ast.literal_eval(tokens) for tokens in texts]

    # delete topics
    for i in range(len(texts)):
        for topic in topics:
            texts[i] = list(filter((topic).__ne__, texts[i]))

    # create n-grams
    texts_with_bigrams = get_ngrams(texts, 2)
    texts_with_trigrams = get_ngrams(texts, 3)

    # remove words that appear only once
    texts = remove_one_appear(texts)
    texts_with_bigrams = remove_one_appear(texts_with_bigrams)
    texts_with_trigrams = remove_one_appear(texts_with_trigrams)
    
    # Create the dictionary
    dictionary = corpora.Dictionary(texts)
    dictionary.filter_extremes(no_below=int(len(texts)*0.01), no_above=0.5)
    corpus = [dictionary.doc2bow(text) for text in texts]
    # print tokens and len of documents
    print(f'Number of unique tokens: {len(dictionary)}')
    print(f'Number of documents: {len(corpus)}')

    dictionary_bi = corpora.Dictionary(texts_with_bigrams)
    dictionary_bi.filter_extremes(no_below=int(len(texts)*0.01), no_above=0.5)
    corpus_bi = [dictionary_bi.doc2bow(text) for text in texts_with_bigrams]
    # print tokens and len of documents
    print(f'Number of unique tokens: {len(dictionary_bi)}')
    print(f'Number of documents: {len(corpus_bi)}')

    dictionary_tri = corpora.Dictionary(texts_with_trigrams)
    dictionary_tri.filter_extremes(no_below=int(len(texts)*0.01), no_above=0.5)
    corpus_tri = [dictionary_tri.doc2bow(text) for text in texts_with_trigrams]
    # print tokens and len of documents
    print(f'Number of unique tokens: {len(dictionary_tri)}')
    print(f'Number of documents: {len(corpus_tri)}')
    
    get_model(dictionary, corpus, texts, titulo)
    get_model(dictionary_bi, corpus_bi, texts_with_bigrams, "bi_"+titulo)
    get_model(dictionary_tri, corpus_tri, texts_with_trigrams, "tri_"+titulo)


def main():
    df = pd.DataFrame()
    for año in range(19,23):
        for mes in range (1,13):
            if mes < 10:
                df_aux = pd.read_csv(f'./csv_revisados/tweets_0{mes}{año}_c.csv')
            else:
                df_aux = pd.read_csv(f'./csv_revisados/tweets_{mes}{año}_c.csv')
            
            df = pd.concat([df,df_aux], sort=False, ignore_index=True)

    df_full = df
    filtro = df['topicQuery'] == 'pirotecnia'
    df_pirotecnia = df[filtro]
    filtro = df['topicQuery'] == 'incendio'
    df_incendio = df[filtro]
    filtro = df['topicQuery'] == 'tránsito'
    df_trafico = df[filtro]

    topics = [
        ['pirotecnia', 'cohete', 'fuego', 'artificial'], 
        ['incendio', 'humo', 'fuego'],
        ['tránsito', 'tráfico']
    ]

    topic_modeling(df_full, topics[0] + topics[1] + topics[2], "")
    topic_modeling(df_pirotecnia, topics[0], "pirotecnia")
    topic_modeling(df_incendio, topics[1], "incendio")
    topic_modeling(df_trafico, topics[2], "trafico")


if __name__ == '__main__':
    #logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)
    main()