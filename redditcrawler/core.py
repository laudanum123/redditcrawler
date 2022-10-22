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
tqdm.pandas()

def pull_reddit_data():
    '''Pulls data from reddit and returns a pandas dataframe with top posts'''
    reddit = praw.Reddit(client_id='-NuD7EbzEKxtmdzCycYLCQ',
                         client_secret='5xx3LIrF_a9s6CLU2K8q46dmzv6l6w',
                         user_agent='USER_AGENT',
                        )

    subreddit = reddit.subreddit('UkrainianConflict')

    hot = subreddit.hot(limit=1000)

    dict =        { "title":[],
                "subreddit":[],
                "score":[], 
                "id":[], 
                "url":[], 
                "comms_num": [], 
                "created": [], 
                "body":[],
                "comments":[]}

    for submission in hot:
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
            dict["comments"].append(submission.comments)

    df = pd.DataFrame(dict)
    df.to_pickle('reddit.pkl')
    
    return df

def read_stored_reddit_data():
    '''Reads data from reddit.csv and returns a pandas dataframe with top posts'''
    df = pd.read_pickle('./reddit.pkl')
    return df



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
        for row in tqdm(rows):
            submission = reddit.submission(id=row[0])
            domain_name = get_fld(submission.url)
            author_name = submission.author.name
            cur.execute('UPDATE submissions SET domain_name=?, author_name=? WHERE id=?', (domain_name, author_name, row[0]))
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

    

if __name__ == '__main__':
    helpers.grab_new_submissions('UkrainianConflict')
    augment_submission_data('UkrainianConflict')
    
