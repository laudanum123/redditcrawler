'''Module for core functionality of redditcrawler.'''
import datetime as dt
import sqlite3
import argparse
import matplotlib.pyplot as plt
import pandas as pd
import praw
from tld import get_fld
from tld.exceptions import TldDomainNotFound, TldBadUrl
from tqdm import tqdm

from redditcrawler import helpers

tqdm.pandas()

def plot_author_distribution(sr_name):
    '''Plots the distribution of authors in the database'''

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


def plot_domain_distribution(sr_name):
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


def plot_time_distribution(sr_name: str):
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

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Reddit Crawler')
    parser.add_argument('--subreddit',
                        type=str,
                        required=False,
                        help='Subreddit to crawl')
    parser.add_argument('--limit',
                        type=int,
                        required=False,
                        help='Limit of submissions to crawl')
    parser.add_argument('--update',
                        default=True,
                        action=argparse.BooleanOptionalAction,
                        help='Only crawl new submissions')
    args = parser.parse_args()
    # todo: add subparser for different functions
    if not args.subreddit:
        args.subreddit = 'rand'
    sr = helpers.grab_new_submissions(args.subreddit,
                                 only_new=args.update,
                                 limit=args.limit)
    plot_author_distribution(sr.name)
    plot_domain_distribution(sr.name)
    plot_time_distribution(sr.name)
