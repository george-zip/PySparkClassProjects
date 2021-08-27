"""
Parse csv file with format id,name,age,number of friends
Print average number of friends by age
"""
from pyspark import SparkConf, SparkContext


# extract age and number of friends
def parse_line(line):
    fields = line.split(',')
    return int(fields[2]), int(fields[3])


sc = SparkContext(conf=SparkConf().setMaster("local").setAppName("friends_by_age"))

lines = sc.textFile("./fakefriends.csv")
extracted_fields = lines.map(parse_line)

# add 1 to each row to count each instance of a particular age then sum number of friends and count fields
totals_by_age = extracted_fields.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
avg_by_age = totals_by_age.mapValues(lambda x: x[0] / x[1]).sortByKey()

for r in avg_by_age.collect():
    print(f"Age: {r[0]}\tAvg friends: {r[1]:.2f}")