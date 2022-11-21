import pyspark
from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("RDD narrow Transformation").getOrCreate()


num1 = spark.sparkContext.parallelize([5, 5, 4, 3, 2, 9, 2])
num2 = spark.sparkContext.parallelize([1, 7, 9, 4, 10, 15])

#intersection function
inter_num = num1.intersection(num2).collect()
print(inter_num)

#subtract
subtract_num = num1.subtract(num2).collect()
#print("substract result:"+subtract_num)

#Distinct
dist_num=num1.distinct(num2).collect()
#print("Result of distinct:"+dist_num)

#countByvalue
data=spark.sparkContext.parallelize([(1,2),(3,4),(3,6),(3,4)]).collect()
#print(data)





