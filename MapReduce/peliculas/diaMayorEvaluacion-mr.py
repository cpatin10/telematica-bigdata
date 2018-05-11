from mrjob.job import MRJob
from mrjob.step import MRStep
import sys

class bestRatingDay(MRJob):
    def steps(self):
        return[
            MRStep(mapper = self.mapper1,
                reducer = self.reducer1),
            MRStep(reducer = self.reducer2)
        ]

    def mapper1(self, _, line):
        parameters = line.split(',')
        yield parameters[-1], float(parameters[-2])

    def reducer1(self, key, values):
        count = 0
        sumRating = 0.0
        for value in values:
            count += 1
            sumRating += value
        yield None, (key, sumRating/count)

    def reducer2(self, _, values):
        dayRating = 0
        bestRatingDay = ""
        for value in values:
            if value[1] > dayRating:
                dayRating = value[1]
                bestRatingDay = value[0]
        yield bestRatingDay, dayRating

if __name__ == '__main__':
    bestRatingDay.run()