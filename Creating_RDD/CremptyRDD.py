from tokenize import String
import pyspark
from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("RDD Test").getOrCreate()
    # Creates empty RDD with no partition createsEmptyRDD[0]
    rdd = spark.sparkContext.emptyRDD()
    print(rdd)
    # Create empty RDD with partition
    rdd2 = spark.sparkContext.parallelize([], 5)
    print(rdd2)
    #To check how partition of RDD
    print("Num of Partitions: " + str(rdd2.getNumPartitions()))
    #To repartition of RDD
    repartision_rdd2=rdd2.repartition(6)
    print("Num of Partitions: "+ str(repartision_rdd2.getNumPartitions()))
    #To minimize the partision of RDD
    coalRDD=rdd2.coalesce(4)
    print("Num of Partitions: "+ str(coalRDD.getNumPartitions()))




