from mrjob.job import MRJob
import sys

class SEMeanSalary(MRJob):
    def mapper1(self, _, line):
        parameters = line.split(',')
        se = parameters[0]
        yield parameters[1], se

    def reducer(self, key, values):
        se = set()
        for value in values:
            se.add(value)
        yield key, len(se)

if __name__ == '__main__':
    SEMeanSalary.run()