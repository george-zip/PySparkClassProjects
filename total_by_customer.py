"""
Summarize total spent by customer
"""
from pyspark import SparkConf, SparkContext


def parse_line(text):
    fields = text.split(',')
    return fields[0], float(fields[2])


sc = SparkContext(conf=SparkConf().setMaster("local").setAppName("TotalByCustomer"))

lines = sc.textFile("./customer-orders.csv")

parsed = lines.map(parse_line)
# definitely not as intuitive as select sum()
total_by_customer = parsed.reduceByKey(lambda x, y: x + y)
total_sorted = total_by_customer.map(lambda t: (t[1], t[0])).sortByKey()

for r in total_sorted.collect():
    print(f"{r[1]}: {r[0]:.2f}")
