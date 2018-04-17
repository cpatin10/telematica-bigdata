from mrjob.job import MRJob
import sys

class StableSuperiorActions(MRJob):
    def mapper(self, _, line):
        parameters = line.split(',')
        value = float(parameters[-2])
        yield parameters[0], value

    def reducer(self, key, values):
        lastValue = -sys.maxint
        decreasingAction = False
        
        for value in values:
            if value < lastValue:
                decreasingAction = True
                break
            else:
                lastValue = value
        if not decreasingAction:
            yield key, leastValue

if __name__ == '__main__':
    StableSuperiorActions.run()