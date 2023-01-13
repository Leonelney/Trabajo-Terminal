import numpy as np
import pandas as pd

df = pd.read_csv('./mysql/author.csv')
# df.drop_duplicates(subset=['authorID'], inplace=True)
df.drop(['authorName'], axis=1, inplace=True)
df.to_csv('./mysql/author_s.csv', index=False)