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
from redditcrawler import plotting

tqdm.pandas()

# todo: add a function to plot the length of submissions
# todo: add a function to print out the most common words in submissions
# todo: add a function to perform a sentiment analysis on submissions

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
    plotting.plot_author_distribution(sr.name)
    plotting.lot_domain_distribution(sr.name)
    plotting.plot_time_distribution(sr.name)
