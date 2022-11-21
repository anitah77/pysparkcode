from pyspark.sql import SparkSession,column
from pyspark.sql.functions import col,lit
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType
if __name__ == '__main__':
    spark=SparkSession.builder.appName("With Coloumn").getOrCreate()

    schema_data=StructType([StructField("id",IntegerType()),
                            StructField("name",StringType()),
                            StructField("city",StringType()),
                               ])
    df = spark.read.load(r"C:\Users\admin\PycharmProjects\sparksession\input_data\emp_assign3.csv",format="csv",schema=schema_data)

    #df.printSchema()
    df.show()
    spark.sql("")
    spark.sql("create external table if not exists empassign(id INT,name STRING,city STRING)partitioned by (curr_date STRING)")
    spark.sql("SET hive.exec.dynamic.partition = true")
    spark.sql("SET hive.exec.dynamic.partition.mode = nonstrict")
    spark.sql("insert into table empassign PARTITION (curr_date) select *, curr_date from emp_assign3").show()