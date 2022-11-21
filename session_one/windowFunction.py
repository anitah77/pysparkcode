from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *
if __name__ == '__main__':

    spark = SparkSession.builder.master("local[*]").appName("Empty Dataframe").getOrCreate()

    schema_data = StructType([StructField("id", IntegerType()),
                              StructField("name", StringType()),
                              StructField("gender", StringType()),
                              StructField("city", StringType()),
                              StructField("salary", DoubleType())
                              ])
    df = spark.read.load(r"C:\Users\admin\PycharmProjects\sparksession\input_data\emp_windowFun.csv",format="csv",schema=schema_data)
    #df.printSchema()
    df.show()

    windowspec=Window.partitionBy("city").orderBy("salary")
    # df.withColumn("row_number",row_number().over(windowspec)).show()
    # df.withColumn("rank",rank().over(windowspec)).show()
    # df.withColumn("dense_rank",dense_rank().over(windowspec)).show()
    df.withColumn("lag",lag("salary",2).over(windowspec)).show()
    df.withColumn("count",count(col("salary")).over(windowspec)).show()

