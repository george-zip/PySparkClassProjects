"""
Tokenize sample book and print out a sorted list of counts by word
"""
import re

from pyspark import SparkConf, SparkContext


def tokenize_text(text):
    # raw string: don't treat backslash as escaping next character
    return re.compile(r"\W+", re.UNICODE).split(text.lower())


sc = SparkContext(conf=SparkConf().setMaster("local").setAppName("CountWords"))

lines = sc.textFile("./book.txt")
one_word_per_line = lines.flatMap(tokenize_text)
word_count = one_word_per_line.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
word_count_sorted = word_count.map(lambda t: (t[1], t[0])).sortByKey()

for r in word_count_sorted.collect():
    count = int(r[0])
    print(f"{r[1]}\t{count}")
