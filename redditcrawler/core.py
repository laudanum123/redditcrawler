import datetime as dt
import sqlite3
from platform import python_branch
from traceback import print_list
from typing import cast

import matplotlib.pyplot as plt
import pandas as pd
import praw
from tld import get_fld
from tqdm import tqdm
from psaw import PushshiftAPI

import helpers

tqdm.pandas()

def augment_submission_data(subreddit_name):
    '''Adds augmented columns for all submissions not yet processed in the database'''

    reddit = praw.Reddit(client_id='-NuD7EbzEKxtmdzCycYLCQ',
                         client_secret='5xx3LIrF_a9s6CLU2K8q46dmzv6l6w',
                         user_agent='USER_AGENT',
                        )
    api = PushshiftAPI(reddit)

    reddit.subreddit(subreddit_name)

    con = sqlite3.connect('data/db/reddit.db')
    cur = con.cursor()
    try:
        cur.execute('SELECT * FROM submissions WHERE subreddit=? AND domain_name IS NULL AND url IS NOT NULL', (subreddit_name,))
        rows = cur.fetchall()
        for i, row in enumerate(tqdm(rows)):
            try:
                domain_name = get_fld(row[4])
            except:
                domain_name = "None"
            submission = reddit.submission(id=row[0])
            author_name = submission.author.name if submission.author else None
            cur.execute('UPDATE submissions SET domain_name=?, author_name=? WHERE id=?', (domain_name, author_name, row[0]))
            if (i+1) % 50 == 0:
                con.commit()
        con.commit()    
        con.close()
    except sqlite3.DatabaseError as e:
        print('Error: ', e)
    
    """ nltk.download('vader_lexicon')
    from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA

    sia = SIA()
    results = []

    for line in df.title:
        pol_score = sia.polarity_scores(line)
        pol_score['headline'] = line
        results.append(pol_score)
    """

def plot_author_distribution(subreddit_name):
    '''Plots the distribution of authors in the database'''
    con = sqlite3.connect('data/db/reddit.db')
    cur = con.cursor()
    cur.execute('SELECT author_name, COUNT(*) FROM submissions WHERE subreddit=? GROUP BY author_name ORDER BY COUNT(*) DESC LIMIT 100', (subreddit_name,))
    rows = cur.fetchall()
    con.close()
    df = pd.DataFrame(rows, columns=['author', 'count'])
    df['percentage'] = df['count'] / df['count'].sum()
    df.plot.bar(x='author', y='percentage', title='{} Author Distribution'.format(subreddit_name), figsize=(20,15))
    plt.savefig('./output/{}_author_distribution.png'.format(subreddit_name))


def plot_domain_distribution(subreddit_name):
    '''Plots the distribution of domains in the database'''
    con = sqlite3.connect('data/db/reddit.db')
    cur = con.cursor()
    cur.execute('SELECT domain_name, COUNT(*) FROM submissions WHERE subreddit=? GROUP BY domain_name ORDER BY COUNT(*) DESC LIMIT 100', (subreddit_name,))
    rows = cur.fetchall()
    con.close()
    df = pd.DataFrame(rows, columns=['domain', 'count'])
    df['percentage'] = df['count'] / df['count'].sum()
    df.plot.bar(x='domain', y='percentage', title='{} Author Distribution'.format(subreddit_name), figsize=(20,15))
    plt.savefig('./output/{}_domain_distribution.png'.format(subreddit_name))

def plot_time_distribution(subreddit_name: str):
    '''Plots the distribution of submissions over time in the database'''
    con = sqlite3.connect('data/db/reddit.db')
    cur = con.cursor()
    cur.execute('SELECT created FROM submissions WHERE subreddit=?', (subreddit_name,))
    rows = cur.fetchall()
    con.close()
    df = pd.DataFrame(rows, columns=['created'])
    df['created'] = pd.to_datetime(df['created'])
    df = df.groupby(pd.Grouper(key='created', freq='D'))['created'].count()

    fig = plt.figure()
    ax = fig.add_subplot(111)
    df.plot.line(x='created', y='count', title='{} Time Distribution'.format(subreddit_name), style='.-', legend=False, figsize=(20,10))
    
    for i,j in df.items():
        ax.annotate(str(j),xy=(i,j))  # type: ignore
        if i.weekday() == 5:  # type: ignore
            ax.axvspan(i, i + dt.timedelta(days=1), alpha=0.2, color='red')  # type: ignore

    plt.savefig('./output/{}_time_distribution.png'.format(subreddit_name))

if __name__ == '__main__':
    subreddit_name = 'AskReddit'
    helpers.grab_new_submissions(subreddit_name, only_new=False, limit=500)
    augment_submission_data(subreddit_name)
    plot_author_distribution(subreddit_name)
    plot_domain_distribution(subreddit_name)
    plot_time_distribution(subreddit_name)
    
