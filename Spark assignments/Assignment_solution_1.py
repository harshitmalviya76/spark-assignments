from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
if __name__=='__main__':
    my_spark = SparkSession.builder.appName("pymongo").config("spark.executor.memory","1g").config("spark.mongodb.read.connection.uri","mongodb+srv://Jayeshagrawal:Bhavini*1@cluster0.xcdfvaa.mongodb.net/?retryWrites=true&w=majority").\
    config("spark.mongodb.write.connection.uri","mongodb+srv://Jayeshagrawal:Bhavini*1@cluster0.xcdfvaa.mongodb.net/?retryWrites=true&w=majority").config("spark.jars.packages","org.mongodb.spark:mongo-spark-connector:10.0.3").getOrCreate()

    casedf=my_spark.read.format("mongodb").option('database','Spark').option('collection','Case').load()
    casedf.select("*").show(50)
    cnt=casedf.select(count("case_id"))
    cnt.show()
    casedf.dropDuplicates().show(100)
    casedf.select("case_id","province","city","group","confirmed").filter("group==True").orderBy(col("confirmed").desc()).limit(10).show()
    casedf.createOrReplaceTempView("case")
    my_spark.sql("select * from case where latitude is Null or longitude is Null limit 10").show()
    casedf.describe().show()
