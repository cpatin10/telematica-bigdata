from mrjob.job import MRJob
import sys

class GreatestValueDay(MRJob):
    def mapper(self, _, line):
        parameters = line.split(',')
        value = float(parameters[-2])
        yield parameters[0], value

    def reducer(self, key, values):
        greatestValue = sys.minint
        for value in values:
            greatestValue = max(greatestValue, value)
        yield key, greatestValue

if __name__ == '__main__':
    GreatestValueDay.run()