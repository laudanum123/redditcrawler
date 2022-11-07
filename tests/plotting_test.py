'''Testing the plotting module.'''
import unittest
import datetime
from os.path import exists
from os import remove
import pandas as pd
from redditcrawler import plotting

class PlottingTests(unittest.TestCase):
    '''Testing the plotting functions in the plotting module.'''

    def test_plot_author_distribution(self):
        '''Test the plot_author_distribution function.'''

        # Arrange
        subreddit_name = 'beziehungen'
        if exists(f'./output/{subreddit_name}_author_distribution.png'):
            remove(f'./output/{subreddit_name}_author_distribution.png')

        # Act
        plotting.plot_author_distribution(subreddit_name)

        # Assert
        self.assertTrue(exists(f'./output/{subreddit_name}_author_distribution.png'))

        # Clean up
        if exists(f'./output/{subreddit_name}_author_distribution.png'):
            remove(f'./output/{subreddit_name}_author_distribution.png')

    def test_plot_domain_distribution(self):
        '''Test the plot_domain_distribution function.'''

        # Arrangj
        subreddit_name = 'UkraineConflict'
        if exists(f'./output/{subreddit_name}_domain_distribution.png'):
            remove(f'./output/{subreddit_name}_domain_distribution.png')

        # Act
        plotting.plot_domain_distribution(subreddit_name)

        # Assert
        self.assertTrue(exists(f'./output/{subreddit_name}_domain_distribution.png'))

        # Clean up
        if exists(f'./output/{subreddit_name}_domain_distribution.png'):
            remove(f'./output/{subreddit_name}_domain_distribution.png')

    def test_plot_time_distribution(self):
        '''Test the plot_time_distribution function.'''

        # Arrangj
        subreddit_name = 'UkraineConflict'
        if exists(f'./output/{subreddit_name}_time_distribution.png'):
            remove(f'./output/{subreddit_name}_time_distribution.png')

        # Act
        plotting.plot_time_distribution(subreddit_name)

        # Assert
        self.assertTrue(exists(f'./output/{subreddit_name}_time_distribution.png'))

        # Clean up
        if exists(f'./output/{subreddit_name}_time_distribution.png'):
            remove(f'./output/{subreddit_name}_time_distribution.png')

    def test_plot_wordcloud(self):
        '''Test the plot_wordcloud function.'''

        # Arrange
        subreddit_name = 'UkraineConflict'
        if exists(f'./output/{subreddit_name}_wordcloud.png'):
            remove(f'./output/{subreddit_name}_wordcloud.png')

        # Act
        plotting.plot_wordcloud(subreddit_name)

        # Assert
        self.assertTrue(exists(f'./output/{subreddit_name}_wordcloud.png'))

if __name__ == '__main__':
    unittest.main()
