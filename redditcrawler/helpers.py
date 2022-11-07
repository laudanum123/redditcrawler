'''This module contains helper functions for the redditcrawler package.'''

import datetime as dt
import sqlite3
from collections import defaultdict
import pandas as pd
import praw
from psaw import PushshiftAPI
from tqdm import tqdm
from tld import get_fld
from tld.exceptions import TldDomainNotFound, TldBadUrl

def convert_datatypes(df_raw):
    '''Converts the datatypes of the columns in the dataframe'''
    convert_dict = {
        'id': str,
        'title': str,
        'subreddit': str,
        'score': int,
        'url': str,
        'comms_num': int,
        'body': str
    }
    df_submission = df_raw.astype(convert_dict)
    return df_submission

def turn_pkl_to_sql(df_raw):
    '''Turns the reddit.pkl file into a sqlite3 database'''
    df_submission = convert_datatypes(df_raw)

    with sqlite3.connect('data/db/reddit.db') as con:
        df_submission.to_sql('submissions', con, if_exists='replace')
        con.commit()
        con.close()

def get_date(created: int):
    '''Converts the created column to a datetime object'''
    return dt.datetime.fromtimestamp(created)

def get_last_submission_from_db(subreddit_name):
    '''Retrieve new submissions for subreddit and add them to the DB'''
    con = sqlite3.connect('data/db/reddit.db')
    cur = con.cursor()
    try:
        cur.execute('SELECT MAX(created) FROM submissions WHERE subreddit=?',
                (subreddit_name, ))
        last_submission = cur.fetchone()[0]
        if last_submission:
            last_submission = dt.datetime.strptime(last_submission,
                                                   '%Y-%m-%d %H:%M:%S')
    except sqlite3.DatabaseError as database_error:
        print('Error: ', database_error)
        last_submission = None
    con.close()
    return last_submission

def get_pushshift_and_praw_instance():
    '''Returns a praw instance wrapped in a PushshiftAPI instance and original praw instance'''
    praw_instance = praw.Reddit(
        client_id='-NuD7EbzEKxtmdzCycYLCQ',
        client_secret='5xx3LIrF_a9s6CLU2K8q46dmzv6l6w',
        user_agent='USER_AGENT',
    )
    ps_instance = PushshiftAPI(praw_instance)
    return ps_instance, praw_instance


def grab_new_submissions(subreddit_name: str,
                         only_new: bool = False,
                         limit: int = 0):
    '''Retrieve new submissions for subreddit and add them to the DB'''
    # Create PushshiftAPI instance
    ps_handler, praw_handler = get_pushshift_and_praw_instance()
    if subreddit_name == 'rand':
        subreddit_name = praw_handler.random_subreddit(nsfw=False)
    if only_new:
        last_submission = get_last_submission_from_db(subreddit_name)
        if last_submission is None:
            print('No submissions found in database for subreddit: ', subreddit_name)
            print('Grabbing all submissions for subreddit: ', subreddit_name)
            last_submission = dt.datetime(1970, 1, 1)
    else:
        last_submission = dt.datetime(1970, 1, 1, 0, 0, 0)

    # Get the new submissions from Reddit
    submissions = ps_handler.search_submissions(after=last_submission,
                                         subreddit=subreddit_name,
                                         limit=limit)

    submission_dict = defaultdict(list)
    print('Grabbing new submissions for subreddit: ', subreddit_name)

    for i, submission_id in enumerate(tqdm(submissions)):
        submission = praw_handler.submission(submission_id)
        if submission.stickied:
            continue
        submission_dict["title"].append(submission.title)
        submission_dict["subreddit"].append(submission.subreddit)
        submission_dict["score"].append(submission.score)
        submission_dict["id"].append(submission.id)
        submission_dict["url"].append(submission.url)
        submission_dict["comms_num"].append(submission.num_comments)
        submission_dict["created"].append(get_date(submission.created))
        submission_dict["body"].append(submission.selftext)
        if submission.author is not None:
            submission_dict["author_name"].append(submission.author.name)
        else:
            submission_dict["author_name"].append('deleted')
        try:
            submission_domain = get_fld(submission.url)
            submission_dict["domain_name"].append(submission_domain)
        except (TldDomainNotFound, TldBadUrl):
            submission_dict["domain_name"].append('')
     
        print(f'{get_date(submission.created)} - {submission.title}')

        if (i + 1) % 20 == 0:
            new_submissions_df = pd.DataFrame(submission_dict)
            new_submissions_df = convert_datatypes(new_submissions_df)

            # Add the new submissions to the database
            con = sqlite3.connect('data/db/reddit.db')
            try:
                lines_added = new_submissions_df.to_sql('submissions',
                                                        con,
                                                        if_exists='append',
                                                        index=False)
                print(f'{lines_added} lines added to the database')
                con.commit()
            except ValueError:
                print('No new submissions')
            con.close()
            submission_dict.clear()
    return subreddit_name
