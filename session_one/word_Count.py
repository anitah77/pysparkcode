from pyspark import SparkContext
from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark=SparkSession.builder.master("local[*]").appName("Different Dataframes").getOrCreate()
    rdd_file = spark.sparkContext.textFile(r"C:\Users\admin\PycharmProjects\sparksession\input_data\emp_word.csv")
    #flatmap_rdd = rdd_file.flatMap(lambda line: line.split(" "))
    rdd_file.collect()