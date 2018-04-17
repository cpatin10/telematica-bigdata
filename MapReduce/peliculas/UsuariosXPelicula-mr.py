from mrjob.job import MRJob

class MoviesXUser(MRJob):
    def mapper(self, _, line):
        parameters = line.split(',')
        rating = float(parameters[-2])
        yield parameters[1], (1, rating)

    def reducer(self, key, values):
        totalUsers = 0
        sumRating = 0.0
        for value in values:
            totalUsers += 1
            sumRating += value[1]
        yield key, (totalUsers, sumRating/totalUsers)

if __name__ == '__main__':
    MoviesXUser.run()