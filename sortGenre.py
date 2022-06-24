# -*- coding: utf-8 -*-
"""
Created on Sun May  1 15:07:17 2022

@author: munat
"""

from mrjob.job import MRJob
from mrjob.step import MRStep

# dictionary with all genres
genres = {
    4: "Unknown", 5: "Action", 6: "Adventure", 7: "Animation", 8: "Children's", 9: "Comedy", 10: "Crime",
    11: "Documentary", 12: "Drama", 13: "Fantasy", 14: "Film-Noir", 15: "Horror", 16: "Musical", 17: "Mystery",
    18: "Romance", 19: "Sci-Fi",
    20: "Thriller", 21: "War", 22: "Western"
}


# to get 10 points
# sort genres by total ratings
class SortGenre(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.sum_rating),
            MRStep(mapper=self.parse_rating,
                   reducer=self.sort_by_total_ratings
                   )
        ]

    def mapper(self, _, row):
        # split row
        data = row.split('\t')
        # check genre in each row
        for i in range(4, len(genres) + 4):
            if data[i] == '1':
                yield genres[i], 1

    # sum ratings
    def sum_rating(self, genre, rating):
        yield None, (sum(rating), genre)

    # convert total ratings to int
    def parse_rating(self, _, values):
        yield None, (int(values[0]), values[1])

    # sort by total ratings
    def sort_by_total_ratings(self, _, values):
        sorted_pairs = sorted(values)
        for pair in sorted_pairs:
            yield pair[1].ljust(12), pair[0]


if __name__ == '__main__':
    SortGenre.run()
