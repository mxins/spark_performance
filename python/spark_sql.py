# -*- coding: utf-8 -*-
# __author__ = 'mxins@qq.com'
from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import os
import time
'''
create table spark.consume_agg
(
  game_id,
  server_id,
  uid,
  register_date,
  date_desc,
  level,
  amount,
  consume_times
)
AS
SELECT t1.game_id,
       t1.server_id,
       t1.uid,
       max(t1.register_date),
       t1.date_desc,
       max(t1.level),
       SUM(t1.amount),
       COUNT(1)
FROM spark.consume_log t1
  JOIN spark.game_dim t2
    ON t1.game_id = t2.game_id
GROUP BY t1.game_id,
         t1.server_id,
         t1.date_desc,
         t1.uid;
'''

spark = SparkSession\
    .builder\
    .appName("Python Spark SQL Test")\
    .getOrCreate()
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
output_dir = base_dir + '/output/'


def save_df(df, table_name):
    df.write.mode('overwrite').csv(output_dir + table_name + '/', timestampFormat="yyyy-MM-dd HH:mm:ss")


def timer(table_name):
    def timeit(func):
        def wrapper(*args):
            t1 = time.time()
            df = func(*args)
            t2 = time.time()
            print('table: %s, count: %d, save df cost time: %r seconds, count cost time: %r seconds, '
                  'total: %r seconds.\n' % (table_name, df.count(), t2 - t1, time.time() - t2, time.time() - t1))
            return df
        return wrapper
    return timeit


class ETL(object):

    @staticmethod
    @timer('game_dim')
    def get_game_dim():
        t1 = time.time()
        game_path = base_dir + '/data/game_dim/'
        game_fields = [StructField('game_id', IntegerType(), True), StructField('game_name', StringType(), True)]
        game_schema = StructType(game_fields)
        game = spark.read.csv(game_path, game_schema)
        t2 = time.time()
        game.createOrReplaceTempView('game_dim')
        t3 = time.time()
        print('get_game_dim:', t2-t1, t3 - t2)
        return game

    @staticmethod
    @timer('consume_log')
    def get_consume_log():
        t1 = time.time()
        consume_path = base_dir + '/data/consume_log/'
        consume_fields = [StructField('game_id', IntegerType(), True), StructField('server_id', IntegerType(), True),
                          StructField('uid', StringType(), True), StructField('register_date', DateType(), True),
                          StructField('date_desc', DateType(), True), StructField('data_time', TimestampType(), True),
                          StructField('session', StringType(), True), StructField('object', StringType(), True),
                          StructField('amount', FloatType(), True), StructField('level', IntegerType(), True),
                          StructField('coin', IntegerType(), True)]
        consume_schema = StructType(consume_fields)
        consume = spark.read.csv(consume_path, consume_schema)
        t2 = time.time()
        consume.createOrReplaceTempView('consume_log')
        t3 = time.time()
        print('get_consume_log:', t2 - t1, t3 - t2)
        return consume

    @staticmethod
    @timer('consume_agg')
    def get_consume_agg():
        t1 = time.time()
        sql = '''
            SELECT t1.game_id,
                   t1.server_id,
                   t1.uid,
                   max(t1.register_date) AS register_date,
                   t1.date_desc,
                   max(t1.level) AS level,
                   SUM(t1.amount) AS amount,
                   COUNT(1) AS consume_times
            FROM consume_log t1
              JOIN game_dim t2
                ON t1.game_id = t2.game_id
            GROUP BY t1.game_id,
                     t1.server_id,
                     t1.date_desc,
                     t1.uid
        '''
        df = spark.sql(sql)
        t2 = time.time()
        save_df(df, 'consume_agg')
        t3 = time.time()
        print('get_consume_agg:', t2-t1, t3 - t2)
        return df

    @staticmethod
    @timer('consume_agg_raw')
    def get_consume_agg_raw():
        t1 = time.time()
        sql = "SELECT t.game_id,\n" + \
              "       t.server_id,\n" + \
              "       t.uid, \n" + \
              "       year(register_date) AS date_year, \n" + \
              "       month(date_desc) AS date_month,\n" + \
              "       t.level, \n" + \
              "       t.amount, \n" + \
              "       t.consume_times \n" + \
              "FROM (SELECT t1.game_id,\n" + \
              "             t1.server_id,\n" + \
              "             t1.uid,\n" + \
              "             max(t1.register_date) AS register_date,\n" + \
              "             t1.date_desc,\n" + \
              "             max(t1.level) AS level,\n" + \
              "             SUM(t1.amount) AS amount,\n" + \
              "             COUNT(1) AS consume_times\n" + \
              "      FROM consume_log t1\n" + \
              "          JOIN game_dim t2\n" + \
              "              ON t1.game_id = t2.game_id\n" + \
              "      GROUP BY t1.game_id,\n" + \
              "               t1.server_id,\n" + \
              "               t1.date_desc,\n" + \
              "               t1.uid) t"
        df = spark.sql(sql)
        df.cache()
        t2 = time.time()
        save_df(df, 'consume_agg_raw')
        t3 = time.time()
        print('get_consume_agg_raw:', t2-t1, t3 - t2)
        return df

    @staticmethod
    @timer('consume_agg_udf')
    def get_consume_agg_udf():
        t1 = time.time()
        sc = spark.sparkContext
        ctx = SQLContext.getOrCreate(sc)
        ctx.registerFunction('udf_year', lambda register_date: register_date.year, IntegerType())
        ctx.registerFunction('udf_month', lambda date_desc: date_desc.month, IntegerType())
        sql = '''
            SELECT t.game_id, t.server_id, t.uid, udf_year(register_date) AS date_year,
                    udf_year(date_desc) AS date_month, t.level, t.amount, t.consume_times
            FROM (SELECT t1.game_id,
                       t1.server_id,
                       t1.uid,
                       max(t1.register_date) AS register_date,
                       t1.date_desc,
                       max(t1.level) AS level,
                       SUM(t1.amount) AS amount,
                       COUNT(1) AS consume_times
                FROM consume_log t1
                  JOIN game_dim t2
                    ON t1.game_id = t2.game_id
                GROUP BY t1.game_id,
                         t1.server_id,
                         t1.date_desc,
                         t1.uid) t
        '''
        df = spark.sql(sql)
        df.cache()
        t2 = time.time()
        save_df(df, 'consume_agg_udf')
        t3 = time.time()
        print('get_consume_agg_udf:', t2-t1, t3 - t2)
        return df

if __name__ == '__main__':
    start = time.time()
    etl = ETL()
    etl.get_game_dim()
    etl.get_consume_log()
    etl.get_consume_agg()
    etl.get_consume_agg_raw()
    etl.get_consume_agg_udf()
    end = time.time()
    print('job finished. cost time: %r seconds.' % (end - start))
