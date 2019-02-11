import sys
import re
from math import sqrt
import numpy as np
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating
from pyspark.mllib.clustering import KMeans, KMeansModel

if __name__ == '__main__':
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~") 
    sc = SparkContext(appName="hybrid")
        
    # Load and parse the data
    data = sc.textFile("ml-latest-small/ratings.csv")
    header = data.first()

    ratings = data.filter(lambda x: x != header) \
        .map(lambda l: l.split(','))\
        .map(lambda l: Rating(int(l[0]), int(l[1]), float(l[2])))

    # Build the recommendation model using Alternating Least Squares
    rank = 10
    numIterations = 10
    ALS_model = ALS.train(ratings, rank, numIterations)

    # Evaluate the ALS_model on training data
    testdata = ratings.map(lambda p: (p[0], p[1]))
    predictions = ALS_model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
    ratesAndPreds = ratings.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
    print("Mean Squared Error = " + str(MSE))

    # # Save and load ALS_model
    # ALS_model.save(sc, "target/tmp/myCollaborativeFilter")
    
    
    # allUserRecs = ALS_model.recommendForAllUsers(10).take(10)
    
    print("~~~~~~~~~~~~~")
    userRecs = ALS_model.recommendProducts(19, 5)
    products_for_users = ALS_model.recommendProductsForUsers(2)
    print(userRecs)
    print(products_for_users.take(5))
    # print(allUserRecs)
    print("~~~~~~~~~~~~~")
    







    userFeature = ALS_model.userFeatures()
    movieFeature = ALS_model.productFeatures()
    # with open('userFeature1.txt', 'w') as output:
    #     output.write('\n'.join('%s %s' % x for x in userFeature))
    
    # userFeatureVector = userFeature.map(lambda line: line[1])
    # print("############################################################")
    # print(userFeature.take(1))
    # print(movieFeature.take(1))
    # print("############################################################")

    # Build the model (cluster the data)
    clusters_model = KMeans.train(userFeatureVector, 10, maxIterations=10, initializationMode="random")

    # # # Evaluate clustering by computing Within Set Sum of Squared Errors
    # # def error(featureVector):
    # #     center = clusters_model.centers[clusters_model.predict(featureVector)]
    # #     return sqrt(sum([x**2 for x in (featureVector - center)]))

    # # print("*****************")
    # # WSSSE = userFeatureVector.map(lambda featureVector: error(featureVector)).reduce(lambda x, y: x + y)
    # # print("Within Set Sum of Squared Error = " + str(WSSSE))

    userCluster = userFeature.map(lambda x: (clusters_model.predict(x[1]), x[1]))
    # userInfo = ratings.map(lambda r: (r[0], (r[1], r[2]))).join(userCluster)

    def list_add(a,b):
        c = []
        for i in range(len(a)):
            c.append(a[i]+b[i])
        return c

    def list_divide(a,b):
        c = []
        for i in range(len(a)):
            c.append(a[i]/b)
        return c

    def cos_sim(a,b):
        return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))  

    centerVector = userCluster.reduceByKey(list_add)

    centerCount = userCluster.countByKey()

    ClusterUserAvgVector = centerVector.map(lambda (k, v): (k, (v, centerCount[k]))) \
        .mapValues(lambda (x, y): list_divide(x, y))
    

    # recommend for different cluster
    # ClusterUserAvgVector.join()
    for i in ClusterUserAvgVector.collect():
        print(i)
#         cos_sim(i[1], 
    
    print("**********")
    print(ClusterUserAvgVector.take(2))
    # print(centerCount)
    print("**********")
    # print(userInfo.take(1))
      


    
    
    
