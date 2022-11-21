from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("Empty Dataframe").getOrCreate()

    schema_data = StructType([StructField("id", IntegerType()),
                              StructField("name", StringType()),
                              StructField("gender", StringType()),
                              StructField("city", StringType()),
                              StructField("salary", DoubleType())
                              ])
    df = spark.read.load(r"C:\Users\admin\PycharmProjects\sparksession\input_data\emp_filter.csv",format="csv",schema=schema_data)
    #df.printSchema()
    df.show()

    df.select(avg("Salary")).show()
    df.select(sum("Salary")).show()
    df.select(min("Salary")).show()
    df.select(max("Salary")).show()
    df.select(count("Salary")).show()