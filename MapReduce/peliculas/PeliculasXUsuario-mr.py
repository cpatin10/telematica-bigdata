from mrjob.job import MRJob

class MoviesXUser(MRJob):
    def mapper(self, _, line):
        parameters = line.split(',')
        rating = float(parameters[-2])
        yield parameters[0], (1, rating)

    def reducer(self, key, values):
        totalMovies = 0
        sumRating = 0.0
        for value in values:
            totalMovies += 1
            sumRating += value[1]
        yield key, (totalMovies, sumRating/totalMovies)

if __name__ == '__main__':
    MoviesXUser.run()