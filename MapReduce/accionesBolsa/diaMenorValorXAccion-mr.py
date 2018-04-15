from mrjob.job import MRJob
import sys

class leastValueDay(MRJob):
    def mapper(self, _, line):
        parameters = line.split(',')
        value = float(parameters[-2])
        yield parameters[0], value

    def reducer(self, key, values):
        leastValue = sys.maxint
        for value in values:
            leastValue = min(leastValue, value)
        yield key, sumSalary/count

if __name__ == '__main__':
    leastValueDay.run()