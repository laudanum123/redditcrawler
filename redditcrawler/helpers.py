import datetime as dt
import sqlite3

import pandas as pd
import praw
from psaw import PushshiftAPI
from tqdm import tqdm


def convert_datatypes(df):
    '''Converts the datatypes of the columns in the dataframe'''
    convert_dict = {'id': str, 'title': str, 'subreddit': str,
                    'score': int, 'url': str, 'comms_num': int, 'body': str}
    df_submission = df.astype(convert_dict)
    return df_submission


def turn_pkl_to_sql(df):
    '''Turns the reddit.pkl file into a sqlite3 database'''
    df_submission = convert_datatypes(df)

    with sqlite3.connect('data/db/reddit.db') as con:
        df_submission.to_sql('submissions', con, if_exists='replace')
        con.commit()
        con.close()


def get_date(created : int):
    return dt.datetime.fromtimestamp(created)


def grab_new_submissions(subreddit_name: str, only_new: bool = True, limit: int = 0):
    '''Retrieve new submissions for subreddit and add them to the DB'''

    # Get the last submission we have in the database
    if only_new:
        con = sqlite3.connect('data/db/reddit.db')
        cur = con.cursor()
        try:
            cur.execute(
                'SELECT MAX(created) FROM submissions WHERE subreddit=?', (subreddit_name,))
            last_submission = cur.fetchone()[0]
            last_submission = dt.datetime.strptime(
                last_submission, '%Y-%m-%d %H:%M:%S')
        except sqlite3.DatabaseError as e:
            print('Error: ', e)
            last_submission = dt.datetime(1970, 1, 1, 0, 0, 0)
        con.close()
    else:
        last_submission = dt.datetime(1970, 1, 1, 0, 0, 0)

    # Get the new submissions from Reddit
    reddit = praw.Reddit(client_id='-NuD7EbzEKxtmdzCycYLCQ',
                         client_secret='5xx3LIrF_a9s6CLU2K8q46dmzv6l6w',
                         user_agent='USER_AGENT',
                         )
    
    api = PushshiftAPI(reddit)
    
    submissions = api.search_submissions(after=last_submission, subreddit=subreddit_name, limit=limit)
                                         
    dict = {"title": [],
            "subreddit": [],
            "score": [],
            "id": [],
            "url": [],
            "comms_num": [],
            "created": [],
            "body": []}

    for i, submission_id in enumerate(tqdm(submissions)):
        submission = reddit.submission(submission_id)
        if submission.stickied:
            continue
        else:
            dict["title"].append(submission.title)
            dict['subreddit'].append(submission.subreddit)
            dict["score"].append(submission.score)
            dict["id"].append(submission.id)
            dict["url"].append(submission.url)
            dict["comms_num"].append(submission.num_comments)
            dict["created"].append(submission.created)
            dict["body"].append(submission.selftext)
            print(submission.title)
        if (i+1)%20 == 0:
            new_submissions_df = pd.DataFrame(dict)
            new_submissions_df = convert_datatypes(new_submissions_df)
            new_submissions_df["created"] = new_submissions_df['created'].apply(get_date)

            # Add the new submissions to the database
            con = sqlite3.connect('data/db/reddit.db')
            try:
                lines_added = new_submissions_df.to_sql('submissions', con, if_exists='append', index=False)
                print('{} lines added to the database'.format(lines_added)) 
                con.commit()
            except ValueError:
                print('No new submissions')
            con.close()
            new_submissions_df = pd.DataFrame()
