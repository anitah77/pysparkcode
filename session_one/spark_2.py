from pyspark.sql import *
from pyspark.sql import functions as F
if __name__ == '__main__':
    Spark= SparkSession.builder.master("local[*]").appName('column class').getOrCreate()
    input_df2 = Spark.createDataFrame([['Brainwork is best'],['AWS data engineer']]).select(F.size(F.split(F.col("_1"), " "))).show()
    input_df23=input_df2.fla