import sys
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

if __name__ == '__main__':
    sc = SparkContext(appName="ALS")
        
    # Load and parse the data
    # ratings_data = sc.textFile("ml-latest-small/ratings.csv")
    ratings_data = sc.textFile("s3n://data-movie/ml-latest-small/ratings.csv")
    header = ratings_data.first()

    ratings = ratings_data.filter(lambda x: x != header) \
        .map(lambda l: l.split(',')) \
        .map(lambda l: Rating(int(l[0]), str(l[1]), float(l[2]))).cache()

    # Build the recommendation model using Alternating Least Squares
    rank = 10
    numIterations = 10
    model = ALS.train(ratings, rank, numIterations)

    # Save model
    model.save(sc, "target/tmp/myCollaborativeFilter")

