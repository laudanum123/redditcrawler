'''Testing the helpers module.'''
import unittest
import datetime
import pandas as pd
from redditcrawler import helpers

class HelperTests(unittest.TestCase):
    '''Testing the helper functions in the helpers module.'''

    def test_get_last_submission_from_db(self):
        '''Test the get_last_submission_from_db function.'''

        # Arrange
        last_submission = helpers.get_last_submission_from_db('AskReddit')

        # Assert
        self.assertTrue(isinstance(last_submission,datetime.datetime))

    def test_convert_datatypes(self):
        '''Test the convert_datatypes function.'''

        # Arrange
        df_raw = pd.DataFrame({'id': [1, 2, 3],
                               'title': ['a', 'b', 'c'],
                               'subreddit': ['sr1', 'sr2', 'sr3'],
                               'score': [1, 2, 3],
                               'url': ['url1', 'url2', 'url3'],
                               'comms_num': [1, 2, 3],
                               'body': ['body1', 'body2', 'body3']})

        # Act
        df_submission = helpers.convert_datatypes(df_raw)

        # Assert
        self.assertTrue(isinstance(df_submission, pd.DataFrame))

    def test_get_date(self):
        '''Test the get_date function.'''

        # Arrange
        created = 1577836800

        # Act
        date = helpers.get_date(created)

        # Assert
        self.assertTrue(isinstance(date, datetime.datetime))
    
    def test_get_pushshift_and_praw_instance(self):
        '''Test the get_pushshift_and_praw_instance function.'''

        # Act
        ps_instance, praw_instance = helpers.get_pushshift_and_praw_instance()

        # Assert
        self.assertTrue(isinstance(ps_instance, helpers.PushshiftAPI))
        self.assertTrue(isinstance(praw_instance, helpers.praw.Reddit))

    def test_grab_new_submissions(self):
        '''Test the grab_new_submissions function.'''

        # Arrange
        subreddit_name = 'AskReddit'

        # Act
        df_submission = helpers.grab_new_submissions(subreddit_name, limit=1)

        # Assert
        self.assertTrue(isinstance(df_submission, str))

if __name__ == '__main__':
    unittest.main()
