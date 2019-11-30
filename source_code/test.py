# Book: Python Data Anlalytics and Visulization with three module

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

APP_NAME = "My Spark Application"
MASTER = "local[4]"

# logFile = "YOUR_SPARK_HOME/README.md"  # Should be some file on your system
# spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
# logData = spark.read.text(logFile).cache()

conf = SparkConf().setAppName(APP_NAME).setMaster(MASTER)
sc = SparkContext(conf=conf)

# main script
data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)
lineLengths = distData.map(lambda s: s*s)
print(lineLengths)
