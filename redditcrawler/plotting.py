'''Functions related to plotting data from the RedditCrawler script'''

import sqlite3
import matplotlib.pyplot as plt
import pandas as pd
import datetime as dt
from wordcloud import WordCloud
from nltk.corpus import stopwords

def plot_author_distribution(sr_name: str) -> None:
    '''Plot the distribution of authors in a subreddit'''

    con = sqlite3.connect('data/db/reddit.db')
    cur = con.cursor()
    cur.execute(
        'SELECT author_name, COUNT(*) FROM submissions WHERE subreddit=? \
        GROUP BY author_name ORDER BY COUNT(*) DESC LIMIT 100',
        (sr_name, ))
    rows = cur.fetchall()
    con.close()
    if not rows:
        print('No author data found for subreddit: ', sr_name)
        return
    df_author = pd.DataFrame(rows, columns=['author', 'count'])
    df_author['percentage'] = df_author['count'] / df_author['count'].sum()
    df_author.plot.bar(x='author',
                y='percentage',
                title=f'{sr_name} Author Distribution',
                figsize=(20, 15))
    plt.savefig(f'./output/{sr_name}_author_distribution.png')


def plot_domain_distribution(sr_name: str) -> None:
    '''Plots the distribution of domains in the database'''

    con = sqlite3.connect('data/db/reddit.db')
    cur = con.cursor()
    cur.execute(
        'SELECT domain_name, COUNT(*) FROM submissions WHERE subreddit=? \
                GROUP BY domain_name ORDER BY COUNT(*) DESC LIMIT 100',
        (sr_name, ))
    rows = cur.fetchall()
    if not rows:
        print('No domain data found for subreddit: ', sr_name)
        return
    con.close()
    df_domain = pd.DataFrame(rows, columns=['domain', 'count'])
    df_domain['percentage'] = df_domain['count'] / df_domain['count'].sum()
    df_domain.plot.bar(x='domain',
                y='percentage',
                title=f'{sr_name} Author Distribution',
                figsize=(20, 15))
    plt.savefig(f'./output/{sr_name}_domain_distribution.png')


def plot_time_distribution(sr_name: str) -> None:
    '''Plots the distribution of submissions over time in the database'''

    con = sqlite3.connect('data/db/reddit.db')
    cur = con.cursor()
    cur.execute('SELECT created FROM submissions WHERE subreddit=?',
                (sr_name, ))
    rows = cur.fetchall()
    if not rows:
        print('No time data found for subreddit: ', sr_name)
        return
    con.close()
    df_time_dist = pd.DataFrame(rows, columns=['created'])
    df_time_dist['created'] = pd.to_datetime(df_time_dist['created'])
    df_time_dist = df_time_dist.groupby(pd.Grouper(key='created', freq='D'))['created'].count()

    fig = plt.figure()
    ax_plot = fig.add_subplot(111)
    df_time_dist.plot.line(x='created',
                 y='count',
                 title=f'{sr_name} Time Distribution',
                 style='.-',
                 legend=False,
                 figsize=(20, 10))

    for i, j in df_time_dist.items():
        ax_plot.annotate(str(j), xy=(i, j))  # type: ignore
        if i.weekday() == 5:  # type: ignore
            ax_plot.axvspan(i, i + dt.timedelta(days=1), alpha=0.2,
                       color='red')  # type: ignore

    plt.savefig(f'./output/{sr_name}_time_distribution.png')

def plot_wordcloud(sr_name: str) -> None:
    '''Plots a wordcloud of the submissions in the database for a given subreddit'''

    con = sqlite3.connect('data/db/reddit.db')
    cur = con.cursor()
    cur.execute('SELECT title FROM submissions WHERE subreddit=?',
                (sr_name, ))
    rows = cur.fetchall()
    if not rows:
        print('No wordcloud data found for subreddit: ', sr_name)
        return
    con.close()
    df_wordcloud = pd.DataFrame(rows, columns=['title'])
    df_wordcloud['title'] = df_wordcloud['title'].progress_apply(
        lambda x: ' '.join([word for word in x.split() \
                if word not in (stopwords.words('english'))]))
    wordcloud = WordCloud(width=1600, height=800).generate(' '.join(df_wordcloud['title']))
    plt.figure(figsize=(20, 10))
    plt.imshow(wordcloud)
    plt.axis('off')
    plt.savefig(f'./output/{sr_name}_wordcloud.png')
