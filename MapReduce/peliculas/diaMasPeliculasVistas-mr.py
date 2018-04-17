from mrjob.job import MRJob
from mrjob.step import MRStep
import sys

class GreatestValueDay(MRJob):
    def steps(self):
        return[
            MRStep(mapper = self.mapper1,
                reducer = self.reducer1),
            MRStep(reducer = self.reducer2)
        ]

    def mapper1(self, _, line):
        parameters = line.split(',')
        yield parameters[-1], 1

    def reducer1(self, key, values):
        yield None (key, sum(values))

    def reducer2(self, _, values):
        movieCount = 0
        greatestMoviesDay = ""
        for value in values:
            if value[1] > movieCount:
                movieCount = value[1]
                greatestMoviesDay = value[0]
        yield greatestMoviesDay, movieCount

if __name__ == '__main__':
    GreatestValueDay.run()