from platform import python_branch
from traceback import print_list
import praw
import pandas as pd
import datetime as dt
import nltk
from tld import get_fld

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
    df = pd.read_pickle('reddit.pkl')
    return df

def get_date(created):
    return dt.datetime.fromtimestamp(created)

def preprocess_data(df):
    '''Processes the data from the reddit dataframe'''

    reddit = praw.Reddit(client_id='-NuD7EbzEKxtmdzCycYLCQ',
                         client_secret='5xx3LIrF_a9s6CLU2K8q46dmzv6l6w',
                         user_agent='USER_AGENT',
                        )

    reddit.subreddit('UkrainianConflict')

    df["created"] = df['created'].apply(get_date)
    df.drop_duplicates(subset =["id"], inplace = True)

    # Get domain names from urls
    if df['domain_name'].empty:
        df['domain_name'] = df['url'].apply(get_fld)
    
    # Get author names
    if df['author'].empty:
        df['author'] = df['id'].apply(lambda x: reddit.submission(id=x).author)
        df.loc[df['author'].isna() == False, 'author_name'] = df.loc[df['author'].isna() == False]['author'].apply(lambda x: x.name)
        df.loc[df['author'].isna() == True, 'author_name'] = df.loc[df['author'].isna() == True]['author'].apply(lambda x: x)

    
    
    """ nltk.download('vader_lexicon')
    from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA

    sia = SIA()
    results = []

    for line in df.title:
        pol_score = sia.polarity_scores(line)
        pol_score['headline'] = line
        results.append(pol_score)
    """
    return df

    

if __name__ == '__main__':
    df = read_stored_reddit_data()
    results = preprocess_data(df)

    
