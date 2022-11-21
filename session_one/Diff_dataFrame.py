#1.Using rdd from SparkSession
# from pyspark.sql import SparkSession
#
# if __name__ == '__main__':
#
#     spark=SparkSession.builder.master("local[*]").appName("Different Dataframes").getOrCreate()
#
#     columns =[("language","users_count")]
#     data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

#     rdd=spark.sparkContext.parallelize(data)
#     # df1_rdd=rdd.toDF()


#     df1_rdd = rdd.toDF(columns)
#     # df1_rdd.printSchema()
#     # df1_rdd.show()

#2.Using createDataFrame() from SparkSession
# from pyspark.sql import SparkSession
#
# if __name__ == '__main__':
#
#     spark=SparkSession.builder.master("local[*]").appName("Different Dataframes").getOrCreate()
#
#     columns = ["language", "users_count"]
#     data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

#     df2_rdd= spark.createDataFrame(rdd).toDF(*columns)
#     df2_rdd.printSchema()
#     df2_rdd.show()

#3.Create DataFrame from List Collection
# from pyspark.sql import SparkSession
#
# if __name__ == '__main__':
#
#     spark=SparkSession.builder.master("local[*]").appName("Different Dataframes").getOrCreate()
#
#     columns = ["language", "users_count"]
#     data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

#     df2_rdd= spark.createDataFrame(data).toDF(*columns)
#     df2_rdd.printSchema()
#     df2_rdd.show()

#4.Using createDataFrame() with the Row type
from pyspark.sql import SparkSession
from pyspark.sql.types import Row

if __name__ == '__main__':

    spark=SparkSession.builder.master("local[*]").appName("Different Dataframes").getOrCreate()

    columns = ["language", "users_count"]
    data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000")]

    rowData = map(lambda x: Row(*x), data)
    df3 = spark.createDataFrame(rowData, columns)
    df3.printSchema()
    df3.show()

#5.Create DataFrame with schema
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType,StructField, StringType, IntegerType
#
# if __name__ == '__main__':
#
#     spark=SparkSession.builder.master("local[*]").appName("Different Dataframes").getOrCreate()
#
#     data2 = [("James", "", "Smith", "36636", "M", 3000),
#              ("Michael", "Rose", "", "40288", "M", 4000),
#              ("Robert", "", "Williams", "42114", "M", 4000),
#              ("Maria", "Anne", "Jones", "39192", "F", 4000),
#              ("Jen", "Mary", "Brown", "", "F", -1)
#              ]
#
#     schema = StructType([ \
#         StructField("firstname", StringType(), True), \
#         StructField("middlename", StringType(), True), \
#         StructField("lastname", StringType(), True), \
#         StructField("id", StringType(), True), \
#         StructField("gender", StringType(), True), \
#         StructField("salary", IntegerType(), True) \
#         ])
#
#     df = spark.createDataFrame(data=data2, schema=schema)
#     df.printSchema()
#     df.show(truncate=False)

