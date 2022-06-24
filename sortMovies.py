# -*- coding: utf-8 -*-
"""
Created on Sun May  1 13:19:40 2022

@author: munat
"""

from mrjob.job import MRJob
from mrjob.step import MRStep

# sort movies by ratings
class SortMovies(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.map_rating,
                   reducer=self.count_rating),
            MRStep(reducer=self.sort_by_rating_numbers)
        ]

    def map_rating(self, _, row):
        details = row.split('\t')
        # movie id , name and rating
        yield details[1].ljust(6) + details[2].ljust(80), 1

    # to get 6 points
    # count ratings
    def count_rating(self, movie_id, rating):
        yield None, (sum(rating), movie_id)

    # to get 8 points
    # sort movie by total ratings
    def sort_by_rating_numbers(self, _, value):
        sorted_pairs = sorted(value)
        for pair in sorted_pairs:
            yield pair[1], pair[0]


if __name__ == '__main__':
    SortMovies.run()
