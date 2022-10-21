import sqlite3
import pandas as pd
import praw
import datetime as dt
from tqdm import tqdm

def convert_datatypes(df):
    '''Converts the datatypes of the columns in the dataframe'''
    convert_dict = {'id': str, 'title': str, 'subreddit':str, 'score':int, 'url': str, 'comms_num':int, 'body': str}
    df_submission = df.astype(convert_dict)
    return df_submission

def turn_pkl_to_sql(df):
    '''Turns the reddit.pkl file into a sqlite3 database'''
    df_submission = convert_datatypes(df)

    con = sqlite3.connect('reddit.db')
    cur = con.cursor()
    df_submission.to_sql('submissions', con, if_exists='replace')

def grab_new_submissions(subreddit_name):
    '''Retrieve new submissions for subreddit and add them to the DB'''

    # Connect to the database
    con = sqlite3.connect('reddit.db')
    cur = con.cursor()

    # Get the last submission we have in the database
    cur.execute('SELECT MAX(created) FROM submissions WHERE subreddit=?', (subreddit_name,))
    last_submission = cur.fetchone()[0]
    last_submission = dt.datetime.strptime(last_submission, '%Y-%m-%d %H:%M:%S')

    # Get the new submissions
    reddit = praw.Reddit(client_id='-NuD7EbzEKxtmdzCycYLCQ',
                         client_secret='5xx3LIrF_a9s6CLU2K8q46dmzv6l6w',
                         user_agent='USER_AGENT',
                        )
    subred = reddit.subreddit(subreddit_name)
    new_submissions = subred.new(limit=1000)

    dict =        { "title":[],
                "subreddit":[],
                "score":[], 
                "id":[], 
                "url":[], 
                "comms_num": [], 
                "created": [], 
                "body":[]}

    for submission in new_submissions:
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
    

    # Add the new submissions to the database
    for submission in tqdm(new_submissions):
        if dt.datetime.fromtimestamp(submission.created) > last_submission:
            new_submissions_df = pd.DataFrame(dict)
            new_submissions_df = convert_datatypes(new_submissions_df)
            

            cur.execute('INSERT INTO submissions VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                        (submission.id, submission.title, submission.subreddit, submission.score, submission.url, submission.num_comments, submission.created, submission.selftext))
    con.commit()

