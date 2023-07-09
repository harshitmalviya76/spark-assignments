from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
if __name__=='__main__':
    my_spark = SparkSession.builder.appName("pymongo").config("spark.executor.memory","1g").config("spark.mongodb.read.connection.uri","mongodb+srv://Jayeshagrawal:Bhavini*1@cluster0.xcdfvaa.mongodb.net/?retryWrites=true&w=majority").\
    config("spark.mongodb.write.connection.uri","mongodb+srv://Jayeshagrawal:Bhavini*1@cluster0.xcdfvaa.mongodb.net/?retryWrites=true&w=majority").config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector:10.0.3").getOrCreate()

    Regiondf=my_spark.read.format("mongodb").option('database','Spark').option('collection','Region').load()
    casedf=my_spark.read.format("mongodb").option('database','Spark').option('collection','Case').load()
    
    """Regiondf.printSchema()"""
    Regiondf.groupBy("province","city").agg(sum("nursing_home_count").alias("total")).select("province","city","total").show()
    casedf.join(Regiondf,casedf.province==Regiondf.province,"inner").show()
