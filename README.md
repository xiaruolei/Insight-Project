# insight-project

## Project Idea
Scaling Collaborative Filtering with PySpark

## Tech Stack
S3 -> Spark -> HBase -> Phoenix -> frontend

## Data Source
[Movie data](https://grouplens.org/datasets/movielens/)

27,000,000 ratings and 1,100,000 tag applications applied to 58,000 movies by 280,000 users.
(data set is not large)

## Engineering Challenge

To get all combinations of (user, item) pairs, the function uses the crossJoin operation between user DataSet and item DataSet. The crossJoin operation is very expensive, resulting in lots of shuffling and is usually not recommended. For example, a crossJoin between 1000 user rows and 1000 item rows will produce 1,000,000 = (1000 x 1000) combined rows. Even though the function groups the user and item sets into blocks to reduce the possible combinations per crossJoin operations, for a large dataset like ours, the crossJoin operation can still explode.

## Business Value

Achieving personalized experience can attract more users. With an increasing number of items, growing number of users, it is hard to compute their interest. 

## MVP

Data stored in S3 and use Spark to make computation to get rating matrix. Then store this matrix in HBase. Connecting HBase to frontend. So if input an user name, it will give your top 10 movies the user like most. If input a movie name, it will give your users list who maybe interest in this movie.

## Stretch Goals
Use a scheduler to automatically run the system periodically and make frontend to visualize it.
