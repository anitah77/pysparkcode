from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == '__main__':
    Spark = SparkSession.builder.master("local[*]").appName('column class').getOrCreate()

    df = Spark.read.load("C:\input_data_hive\spark_wordcount.txt", format="Text", inferschema=True)
    newDf = df.withColumn('word', F.explode((F.split(F.col("value"), ' ')))).groupBy("word").count().sort("count",
                                                                                                          ascending=False).show()