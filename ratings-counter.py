"""
Print ratings histogram of the MovieLens dataset using an RDD
Dataset is available at https://grouplens.org/datasets/movielens/100k/
"""
from pyspark import SparkConf, SparkContext

# use locally-running Spark instance
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

lines = sc.textFile("./u.data")
# extract ratings column
ratings = lines.map(lambda x: x.split()[2])
count_by_rating = ratings.countByValue()

for key in sorted(count_by_rating):
    print(f"Rating: {key}\tCount: {count_by_rating[key]}")
