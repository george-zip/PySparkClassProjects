"""
Read csv file containing temperature readings and print max temp by station id
Format: station id,date,reading type,observation
"""
from pyspark import SparkContext, SparkConf


def parse_line(line):
    fields = line.split(",")
    # station id, observation type and observation converted from celsius to fahrenheit scale
    return fields[0], fields[2], float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0


sc = SparkContext(conf=SparkConf().setMaster("local").setAppName("FindMaxTemps"))
lines = sc.textFile("./1800.csv")
parsed_lines = lines.map(parse_line)

max_observations = parsed_lines.filter(lambda x: "TMAX" in x[1])
# drop observation type column and find max per station
max_per_station = max_observations.map(lambda x: (x[0], x[2])).reduceByKey(lambda x, y: max(x, y))

for r in max_per_station.collect():
    print(f"Station {r[0]}\tMax: {r[1]:.2f}F")
