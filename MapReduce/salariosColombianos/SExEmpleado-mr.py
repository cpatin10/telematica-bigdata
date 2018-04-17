from mrjob.job import MRJob

class SExEmployee(MRJob):
    def mapper(self, _, line):
        parameters = line.split(',')
        se = parameters[0]
        yield parameters[1], se

    def reducer(self, key, values):
        seSet = set()
        for value in values:
            seSet.add(value)
        yield key, len(seSet)

if __name__ == '__main__':
    SExEmployee.run()