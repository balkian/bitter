import pandas as pd

def read_rts(rtsfile, tweetsfile):
    tweets = pd.read_csv(tweetsfile, index_col=0)
    rts = pd.read_csv(rtsfile, index_col=1)
    merged = rts.groupby(by=['id', 'rt_user_id']).size().rename('count').reset_index(level=1).merge(tweets, left_index=True, right_index=True)
    return merged.sort_values(by='count', ascending=False)


def read_tweets(tweetsfile):
    '''When the dataset is small enough, we can load tweets as-in'''
    with open(tweetsfile) as f:
        header = f.readline().strip().split(',')
        dtypes = {}
    for key in header:
        if key.endswith('_str') or key.endswith('.id'):
            dtypes[key] = object 
            tweets = pd.read_csv(tweetsfile, dtype=dtypes, index_col=0)
    return tweets


if __name__ == '__main__':
    import argparse
