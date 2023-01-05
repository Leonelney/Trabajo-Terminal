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
                            cohere, limit, start=2, step=2):
    coherence_values = []
    chunksize = 3500
    passes = 10
    iterations = 200

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


def get_topic_coherence_score(dictionary, corpus, texts):
    limit=30
    start=2
    step=2

    coherence_values = compute_coherence_values(dictionary=dictionary, 
                                            corpus=corpus, 
                                            texts=texts, 
                                            cohere='c_v', # 'u_mass', 'c_v', 'c_uci', 'c_npmi'
                                            start=start, 
                                            limit=limit, 
                                            step=step)

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
    plt.savefig('./resultados_tm/topic_coherence.png', dpi=300)


def main():
    df = pd.DataFrame()
    for año in range(19,23):
        for mes in range (1,13):
            if mes < 10:
                df_aux = pd.read_csv(f'./csv_revisados/tweets_0{mes}{año}_c.csv')
            else:
                df_aux = pd.read_csv(f'./csv_revisados/tweets_{mes}{año}_c.csv')
            
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
    dictionary.filter_extremes(no_below=65, no_above=0.5)
    # Create the corpus.  This is a Term Frequency 
    # or Bag of Words representation.
    corpus = [dictionary.doc2bow(text) for text in texts]
    # print tokens and len of documents
    print(f'Number of unique tokens: {len(dictionary)}')
    print(f'Number of documents: {len(corpus)}')
    
    # Training the Model
    NUM_TOPICS = 6
    chunksize = 3500
    passes = 10
    iterations = 200
    eval_every = None
    temp = dictionary[0]
    id2word = dictionary.id2token

    model = LdaModel(
        corpus=corpus,
        id2word=id2word,
        chunksize=chunksize,
        alpha='auto',
        eta='auto',
        iterations=iterations,
        num_topics=NUM_TOPICS,
        passes=passes,
        eval_every=eval_every
    )

    # Compute the Best Number of Topics
    #get_topic_coherence_score(dictionary, corpus, texts)

    # feed the LDA model into the pyLDAvis instance
    lda_viz = gensimvis.prepare(model, corpus, dictionary, sort_topics=True)

    pyLDAvis.save_html(lda_viz, './resultados_tm/lda.html')


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)
    main()