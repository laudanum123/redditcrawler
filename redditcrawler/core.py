from platform import python_branch
from traceback import print_list
import praw
import pandas as pd
import datetime as dt
import nltk
from tld import get_fld
import sqlite3
from tqdm import tqdm
import helpers
import matplotlib.pyplot as plt
tqdm.pandas()


def augment_submission_data(subreddit_name):
    '''Adds augmented columns for all submissions not yet processed in the database'''

    reddit = praw.Reddit(client_id='-NuD7EbzEKxtmdzCycYLCQ',
                         client_secret='5xx3LIrF_a9s6CLU2K8q46dmzv6l6w',
                         user_agent='USER_AGENT',
                        )

    reddit.subreddit(subreddit_name)

    con = sqlite3.connect('reddit.db')
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
    con = sqlite3.connect('./reddit.db')
    cur = con.cursor()
    cur.execute('SELECT author_name, COUNT(*) FROM submissions WHERE subreddit=? GROUP BY author_name ORDER BY COUNT(*) DESC LIMIT 100', (subreddit_name,))
    rows = cur.fetchall()
    con.close()
    df = pd.DataFrame(rows, columns=['author', 'count'])
    df['percentage'] = df['count'] / df['count'].sum()
    df.plot.bar(x='author', y='percentage')
    plt.show()

def plot_domain_distribution(subreddit_name):
    '''Plots the distribution of domains in the database'''
    con = sqlite3.connect('./reddit.db')
    cur = con.cursor()
    cur.execute('SELECT domain_name, COUNT(*) FROM submissions WHERE subreddit=? GROUP BY domain_name ORDER BY COUNT(*) DESC LIMIT 100', (subreddit_name,))
    rows = cur.fetchall()
    con.close()
    df = pd.DataFrame(rows, columns=['domain', 'count'])
    df['percentage'] = df['count'] / df['count'].sum()
    df.plot.bar(x='domain', y='percentage')
    plt.show()

if __name__ == '__main__':
    subreddit_name = 'UkraineConflict'
    helpers.grab_new_submissions(subreddit_name, limit=None)
    augment_submission_data(subreddit_name)
    plot_author_distribution(subreddit_name)
    plot_domain_distribution(subreddit_name)
    
