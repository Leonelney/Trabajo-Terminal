import numpy as np
import pandas as pd
import sys
import json
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


def get_graphic_coherencia(coherence_values, start, step, limit, titulo):
    plt.figure(figsize=(8,5))

    # Create a custom x-axis
    x = range(start, limit, step)

    # Build the line plot
    ax = sns.lineplot(x=x, y=coherence_values, color='#238C8C')

    # Set titles and labels
    plt.title("Best Number of Topics for LDA Model")
    plt.xlabel("Num Topics")
    plt.ylabel("Coherence score")
    plt.xlim(start, limit)
    plt.xticks(range(2, limit, step))

    # Add a vertical line to show the optimum number of topics
    plt.axvline(x[np.argmax(coherence_values)], 
                color='#F26457', linestyle='--')

    # Draw a custom legend
    legend_elements = [Line2D([0], [0], color='#238C8C', 
                            ls='-', label='Coherence Value (c_v)'),
                    Line2D([0], [1], color='#F26457', 
                            ls='--', label='Optimal Number of Topics')]

    ax.legend(handles=legend_elements, loc='upper right')
    
    plt.tight_layout()
    plt.savefig(f'./04_lda_results/topic_coherence_{titulo}.png', dpi=300)


def get_topic_coherence_score(dictionary, corpus, texts, chunksize, passes, iterations, titulo):
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

    get_graphic_coherencia(coherence_values, start, step, limit, titulo)

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


def create_table_metadata(model, corpus, num_topics):
    terms_per_topic = {}
    
    for i in range(num_topics):
        terms_per_topic[i+1] = model.show_topic(i, 30)

    topics_per_document = list(model.get_document_topics(corpus))

    df_data = pd.read_csv('./03_tables/dim_metadata.csv')
    df_topics = pd.DataFrame()
    df_pivot_topics = pd.DataFrame()

    topic_id = []
    topic_terms = []
    topic_relevance = []
    for key, value in terms_per_topic.items():
        for word, relevance in value:
            topic_id.append(key)
            topic_terms.append(word)
            topic_relevance.append(float(relevance))
    df_topics['topicID'] = topic_id
    df_topics['topic_terms'] = topic_terms
    df_topics['terms_relevance'] = topic_relevance
    df_topics.to_csv('./03_tables/dim_topic.csv', index=False)

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
            prevalence.append(float(topic[1]))
            idx += 1
    df_pivot_topics['pivotID'] = pivot_id
    df_pivot_topics['pubID'] = pub_id
    df_pivot_topics['topicID'] = topic_id
    df_pivot_topics['prevalence'] = prevalence
    df_pivot_topics.to_csv('./03_tables/hechos_topic.csv', index=False)


def get_model(dictionary, corpus, texts, titulo):
    # Training the Model
    chunksize = int(len(texts)/2)
    passes = int(sys.argv[3])
    iterations = int(sys.argv[4])
    eval_every = None
    temp = dictionary[0]
    
    if sys.argv[2] == "-t":
        NUM_TOPICS = get_topic_coherence_score(dictionary, corpus, texts, chunksize, passes, iterations, titulo)
    if sys.argv[2] == "-p":
        NUM_TOPICS = 4
    if sys.argv[2] == "-c":
        NUM_TOPICS = int(sys.argv[5])
    
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

    if sys.argv[2] == "-c":
        # feed the LDA model into the pyLDAvis instance
        lda_viz = gensimvis.prepare(model, corpus, dictionary, sort_topics=False)
        pyLDAvis.save_html(lda_viz, f'./04_lda_results/lda_{titulo}.html')

        # crea la tablas de dim_topic y hechos_topic
        create_table_metadata(model,corpus,NUM_TOPICS)


def init(texts, titulo):
    # Create the dictionary
    dictionary = corpora.Dictionary(texts)
    dictionary.filter_extremes(no_below=250, no_above=0.5)
    corpus = [dictionary.doc2bow(text) for text in texts]
    # print tokens and len of documents
    print(f'Number of unique tokens: {len(dictionary)}')
    print(f'Number of documents: {len(corpus)}')

    get_model(dictionary, corpus, texts, titulo)


def topic_modeling(df, topics, titulo):
    texts = list(df['tokens'])
    texts = [str(tokens).split(",") for tokens in texts]
    
    # delete topics
    for i in range(len(texts)):
        for topic in topics[df.loc[i,"topicQuery"]]["sinonimos_tokens"]:
            texts[i] = list(filter((topic).__ne__, texts[i]))

    # create n-grams
    texts_with_bigrams = get_ngrams(texts, 2)
    texts_with_trigrams = get_ngrams(texts, 3)

    # remove words that appear only once
    texts = remove_one_appear(texts)
    texts_with_bigrams = remove_one_appear(texts_with_bigrams)
    texts_with_trigrams = remove_one_appear(texts_with_trigrams)
    
    if sys.argv[1] == "-u":
        init(texts, titulo)
    elif sys.argv[1] == "-b":
        init(texts_with_bigrams, "bi_"+titulo)
    elif sys.argv[1] == "-t":
        init(texts_with_trigrams, "tri_"+titulo)
    else:
        print("ERROR: no se selecciono el tipo de token")


def main():
    # creamos un dataframe a partir de la tabla de metadatos y la tabla de hechos
    df_metadata = pd.read_csv(f'./03_tables/dim_metadata.csv')
    df_hechos = pd.read_csv(f'./03_tables/dim_hechos.csv')
    df = df_metadata[['pubID','tokens']]
    df.insert(1, 'topicQuery', df_hechos['topicQuery'])

    # creamos diferentes dataframes para cada uno de los temas
    filtro = df['topicQuery'] == 'pirotecnia'
    df_pirotecnia = df[filtro]
    filtro = df['topicQuery'] == 'incendio'
    df_incendio = df[filtro]
    filtro = df['topicQuery'] == 'tránsito'
    df_trafico = df[filtro]

    # abrimos el csv con los topics
    with open("./00_querys/topics.json") as file:
        parameters = json.load(file)

    topic_modeling(df, parameters['topics'], "general")
    # topic_modeling(df_pirotecnia, topics[0], "pirotecnia")
    # topic_modeling(df_incendio, topics[1], "incendio")
    # topic_modeling(df_trafico, topics[2], "trafico")


if __name__ == '__main__':
    # logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)
    main()