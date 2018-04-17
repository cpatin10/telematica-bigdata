from mrjob.job import MRJob
from mrjob.step import MRStep
import sys

class leastValueDay(MRJob):

    def steps(self):
        return[
            MRStep(mapper = self.mapper1,
                reducer = self.reducer1),
            MRStep(reducer = self.reducer2)
        ]

    def mapper1(self, _, line):
        parameters = line.split(',')
        value = float(parameters[-2])
        yield parameters[0], (value, parameters[-1])

    def reducer1(self, key, values):
        leastValue = sys.maxint
        leastValueDate = ""
        for value in values:
            if value[0] < leastValue:
                leastValue = value[0]
                leastValueDate = value[1]
        yield leastValueDate, 1

    def reducer2(self, key, values):
        yield key, sum(values)

    

if __name__ == '__main__':
    leastValueDay.run()