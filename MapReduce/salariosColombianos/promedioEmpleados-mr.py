from mrjob.job import MRJob

class SEMeanSalary(MRJob):
    def mapper(self, _, line):
        parameters = line.split(',')
        salary = int(parameters[-2])
        yield parameters[1], salary

    def reducer(self, key, values):
        count = 0
        sumSalary = 0
        for value in values:
            count += 1
            sumSalary += value
        yield key, sumSalary/count

if __name__ == '__main__':
    SEMeanSalary.run()