''' Module for listing of subreddits in database. '''
import sqlite3

def list_parser(args):
    '''List subreddits in database.'''
    list_subreddits()

def list_subreddits():
    '''List subreddits in database.'''
    conn = sqlite3.connect('data/db/reddit.db')
    cursor = conn.cursor()
    cursor.execute('SELECT subreddit, count(*) FROM submissions GROUP BY \
                   subreddit ORDER BY count(*) DESC')
    query_results = cursor.fetchall()
    for result in query_results:
        print(result)
    conn.close()
