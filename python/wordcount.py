# -*- coding: utf-8 -*-
# __author__ = 'mxins@qq.com'
from __future__ import print_function
import time
import os
from operator import add
from pyspark.sql import SparkSession
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
path = base_dir + '/data/consume_log/'
output_dir = base_dir + '/output/'


if __name__ == "__main__":
    t1 = time.time()
    spark = SparkSession\
        .builder\
        .appName("PythonWordCount")\
        .getOrCreate()

    lines = spark.read.text(path).rdd.map(lambda r: r[0])
    counts = lines.flatMap(lambda x: x.split(',')) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)

    # output = counts.collect()
    # for (word, count) in output:
    #     print("%s: %i" % (word, count))
    t2 = time.time()
    counts.saveAsTextFile(output_dir + 'python_wordcount/')
    t3 = time.time()
    print('python wordcount, cost:', t2 - t1, t3 - t2)

    spark.stop()
