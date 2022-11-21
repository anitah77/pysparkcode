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
    df.printSchema()
    #df.show()

    #Filter
    #df.filter(df.gender=="Male").show()
    df.filter(df.gender=="Male").select("id","name","gender").show()
    df.filter(df.gender!="Male").select("id","name","gender").show()
    #salary less than 10000
    df.filter(df.salary<10000).show()

    df.filter(df.gender.startswith("M")).show()
