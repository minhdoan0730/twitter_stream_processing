from data_stream import DataSource

from collections import namedtuple

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext, SparkSession, Row
from pyspark.sql.functions import desc
from pyspark.sql.types import *

import sys
import requests

# -------------- Spark App config and init ---------------- #
APP_NAME = "My Spark Application"
MASTER = "local[4]"
# conf = SparkConf().setAppName(APP_NAME).setMaster(MASTER)
# sc = SparkContext(conf=conf)


def hashtags_count_in_tweets(df):
    '''
    - Input: A data frame with tweet data
    - Output: Return a list of hashtags trending in 2009
    '''
    hashtags_rdd = df.select("text").rdd \
        .flatMap(lambda line: line["text"].split(" ")) \
        .filter(lambda w: '#' in w) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a,b: a + b)
    hashtags_rdd.toDF().show()
# end

# ------------------------------------------------- #
data_dir = '/home/minhdn/code/big_data/data/'
data_filename = 'testdata_manual_2009_06_14.csv'
if __name__ == "__main__":
    my_spark = SparkSession \
        .builder \
        .appName("myApp") \
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/sparkkafka.stream") \
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/sparkkafka.stream") \
        .getOrCreate()
    sql_context = SQLContext(my_spark)
    sc = my_spark.sparkContext
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 10)

    # Lazily instantiated global instance of SparkSession
    def getSparkSessionInstance(sparkConf):
        if ("sparkSessionSingletonInstance" not in globals()):
            globals()["sparkSessionSingletonInstance"] = SparkSession \
                .builder \
                .config(conf=sparkConf) \
                .getOrCreate()
        return globals()["sparkSessionSingletonInstance"]

    def process(time, rdd):
        print("========= %s =========" % str(time))
        try:
            # Get the singleton instance of SparkSession
            spark = getSparkSessionInstance(rdd.context.getConf())

            # Convert RDD[String] to RDD[Row] to DataFrame
            row_rdd = rdd.map(lambda w: Row(hashtag=w))
            hashtag_df = spark.createDataFrame(row_rdd)

            # Creates a temporary view using the DataFrame
            hashtag_df.createOrReplaceTempView("hashtags")

            # Do word count on table using SQL and print it
            hashtag_df = spark.sql("""
                SELECT hashtag, count(*) AS hashtag_count
                FROM hashtags
                GROUP BY hashtag
            """)
            hashtag_df.show()
            send_df_to_dashboard(hashtag_df)
        except:
            pass

    def send_df_to_dashboard(df):
        print("========== ok send data to dashboard ===========")
        top_tags = [str(t.hashtag) for t in df.select("hashtag").collect()]
        tags_count = [p.hashtag_count for p in df.select("hashtag_count").collect()]

        # initialize and send the data through REST API
        url = 'http://localhost:5000/updateData'
        request_data = {'label': str(top_tags), 'data': str(tags_count)}
        response = requests.post(url, data=request_data)
    # end

    lines = ssc.socketTextStream("127.0.0.1", 5556)
    hashtags = lines.flatMap(
        lambda text: text.split(" ")
    ).filter(lambda w: w.lower().startswith("#"))

    hashtags.foreachRDD(process)

    # Create an sql context so that we can query data files in sql
    # data_src = DataSource(data_dir, data_filename)
    # df = sql_context.read.load(
    #     data_src.get_path(),
    #     format='com.databricks.spark.csv',
    #     header='true',
    #     inferSchema='true'
    # )
    # hashtags_count_in_tweets(df)

    # You must start the Spark StreamingContext, and await process termination
    ssc.start()
    ssc.awaitTermination()
