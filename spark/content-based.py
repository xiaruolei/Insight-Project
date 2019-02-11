from pyspark import SparkContext
import sys
from operator import add
import re
from math import sqrt
import math
from pyspark.mllib.clustering import KMeans, KMeansModel
from numpy import array
from math import sqrt

from pyspark.mllib.clustering import KMeans, KMeansModel

def getYears(title):
    p = re.compile(r"(?:\((\d{4})\))?\s*$")
    m = p.search(title)
    year = m.group(1)
    if year:
        year = int(year)
    else:
        year = 0
    return year

def getGenreVector(genres):
    mapping = {"Action": 0, "Adventure": 1, "Animation": 2, "Children": 3, "Comedy": 4, "Crime": 5, "Documentary": 6, "Drama": 7, "Fantasy": 8, "Film-Noir": 9, "Horror": 10, "Musical": 11, "Mystery": 12, "Romance": 13, "Sci-Fi": 14, "Thriller": 15, "War": 16, "Western": 17}
    vector = [0] * 18
    genreList = genres.split('|')
    for genre in genreList:
        if genre in mapping:
            index = mapping[genre]
            vector[index] += 1
    return vector

def computeYearSimilarity(year1, year2):
    diff = abs(year1 - year2)
    sim = math.exp(-diff / 10.0)
    return sim

def computeGenreSimilarity(vector1, vector2):
    sumxx, sumxy, sumyy = 0, 0, 0
    for i in range(len(vector1)):
        x = vector1[i]
        y = vector2[i]
        sumxx += x * x
        sumyy += y * y
        sumxy += x * y
    
    return sumxy/math.sqrt(sumxx*sumyy)

def concatenate(l):
    if len(l) == 2:
        return l
    elif len(l) > 2:
        return (l[0], ' '.join(x for x in l[1:-1]), l[-1])

if __name__ == '__main__':
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~") 
    sc = SparkContext(appName="content-based")
        
    # Load and parse the data
    data = sc.textFile("ml-latest-small/movies.csv")
    header = data.first()

    movies = data.filter(lambda x: x != header) \
        .map(lambda l: l.replace('\"', '')) \
        .map(lambda l: l.split(',')) \
        .map(lambda l: concatenate(l)) \
        .filter(lambda l: l[2] != '(no genres listed)') \
        .map(lambda l: (int(l[0]), getYears(l[1]), getGenreVector(l[2]))) \

    movieFeatureVector = movies.map(lambda l: array(l[2]))
    print(movieFeatureVector.take(1))
    
    # Build the model (cluster the data)
    clusters_model = KMeans.train(movieFeatureVector, 30, maxIterations=10, initializationMode="random")  
    movieCluster = movies.map(lambda l: (clusters_model.predict(array(l[2])), (l[0], l[1], l[2])))
    print("#######")
    print(movieCluster.take(1))
    print("#######")

    # # Evaluate clustering by computing Within Set Sum of Squared Errors
    # def error(point):
    #     center = clusters_model.centers[clusters_model.predict(point)]
    #     return sqrt(sum([x**2 for x in (point - center)]))

    # WSSSE = movieFeatureVector.map(lambda point: error(point)).reduce(lambda x, y: x + y)
    # print("Within Set Sum of Squared Error = " + str(WSSSE))


    joinedMovies = movieCluster.groupBy(lambda x: x[0]) \
        .mapValues(lambda x: x.cartesian(x)) \
        .take(10)

    # joinedMovies = movies.cartesian(movies).filter(lambda (x, y): x < y)
    # # joinedMovies.map(lambda (m1, m2): m1)
    # simMovies = joinedMovies.map(lambda (m1, m2): ((m1[0], m2[0]), computeYearSimilarity(m1[1], m2[1]) * computeGenreSimilarity(m1[2], m2[2]))) \
    #     .map(lambda (pair, simScore): (pair[0], pair[1], simScore)) \
    #     .groupBy(lambda x: x[0]) \


    # def sum(l):
    #     res = 0
    #     for i in l:
    #         res += i
    #     return res

    # bug = movies.map(lambda l: (l[0], sum(l[2])))
    # for i in bug.collect():
    #     if i[1] == 0:
    #         print(i)
    
    # for i in movies.collect():
    #     print(i)



    # for i in simMovies.take(10000):
    #     print(i)
    # print(simMovies.take(1))





