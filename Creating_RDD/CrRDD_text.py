import pyspark
from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("RDD Test").getOrCreate()
    input_rdd=spark.sparkContext.textFile("C:\input_data_hive\employee.txt")
    print(input_rdd.collect())
