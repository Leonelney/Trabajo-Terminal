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
    limit=10
    start=1
    step=1

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

    print(coherence_values)
    return (coherence_values.index(max(coherence_values))+1)
    

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

def create_table_metadata(model,corpus):
    terms_per_topic = {}
    for i in range(4):
        terms_per_topic[i+1] = model.show_topic(i, 20)

    topics_per_document = list(model.get_document_topics(corpus))
    print(topics_per_document)

    df_data = pd.read_csv('./mysql/data.csv')
    df_topics = pd.DataFrame()
    df_pivot_topics = pd.DataFrame()

    topic_id = []
    topic_terms = []
    for key, value in terms_per_topic.items():
        topic_id.append(key)
        topic_terms.append(value)
    df_topics['topicID'] = topic_id
    df_topics['terms_relevance'] = topic_terms
    df_topics.to_csv('./mysql/topic.csv', index=False)

    idx = 1
    pivot_id = []
    pub_id = []
    topic_id = []
    prevalence = []
    for i in range(len(topics_per_document)):
        for topic in topics_per_document[i]:
            pivot_id.append(f't{idx}')
            pub_id.append(df_data['pubID'][i])
            topic_id.append(topic[0]+1)
            prevalence.append(topic[1])
            idx += 1
    df_pivot_topics['pivotID'] = pivot_id
    df_pivot_topics['pubID'] = pub_id
    df_pivot_topics['topicID'] = topic_id
    df_pivot_topics['prevalence'] = prevalence
    df_pivot_topics.to_csv('./mysql/main_topic.csv', index=False)

def get_model(dictionary, corpus, texts, titulo):
    # Training the Model
    chunksize = int(len(texts)/2)
    passes = 7
    iterations = 110
    eval_every = None
    temp = dictionary[0]
    
    # NUM_TOPICS = get_topic_coherence_score(dictionary, corpus, texts, chunksize, passes, iterations)
    NUM_TOPICS = 4
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

    create_table_metadata(model,corpus)

def init(texts, titulo):
    # Create the dictionary
    dictionary = corpora.Dictionary(texts)
    dictionary.filter_extremes(no_below=int(len(texts)*0.01), no_above=0.5)
    corpus = [dictionary.doc2bow(text) for text in texts]
    # print tokens and len of documents
    print(f'Number of unique tokens: {len(dictionary)}')
    print(f'Number of documents: {len(corpus)}')

    get_model(dictionary, corpus, texts, titulo)

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
    
    # init(texts, titulo)
    # init(texts_with_bigrams, "bi_"+titulo)
    init(texts_with_trigrams, "tri_"+titulo)


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
    # topic_modeling(df_pirotecnia, topics[0], "pirotecnia")
    # topic_modeling(df_incendio, topics[1], "incendio")
    # topic_modeling(df_trafico, topics[2], "trafico")


if __name__ == '__main__':
    # logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)
    main()