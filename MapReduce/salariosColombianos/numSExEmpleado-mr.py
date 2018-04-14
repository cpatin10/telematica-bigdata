from mrjob.job import MRJob

class SEMeanSalary(MRJob):
    def mapper1(self, _, line):
        parameters = line.split(',')
        yield parameters[1], parameters[0]

    def reducer(self, key, values):
        se = set()
        for value in values:
            se.add(value)
        yield key, len(se)

if __name__ == '__main__':
    SEMeanSalary.run()