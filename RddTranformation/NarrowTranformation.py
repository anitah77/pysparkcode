import pyspark
from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("RDD narrow Transformation").getOrCreate()
    rdd=spark.sparkContext.parallelize([5,5,4,3,2,9,2])

    #map() function
    map_rdd1=rdd.map(lambda x:x*2).collect()
    map_rdd2=rdd.map(lambda x:pow(x,2)).collect()
    print(map_rdd1) #op-[10, 10, 8, 6, 4, 18, 4]
    print(map_rdd2) #op-[25, 25, 16, 9, 4, 81, 4]


    #flatMap() function
    frdd=spark.sparkContext.parallelize([1,2,3,4])
    flatmap_rdd1=frdd.flatMap(lambda x:(x,x*10,57)).collect()
    print(flatmap_rdd1) #op-[1, 10, 57, 2, 20, 57, 3, 30, 57, 4, 40, 57]
    flatmap_rdd2=frdd.flatMap(lambda x:range(1,x)).collect()
    print(flatmap_rdd2) #op-[1, 1, 2, 1, 2, 3]

    #Filter() function
    filter_rdd=rdd.filter(lambda x:x%2==0).collect()
    print(filter_rdd) #op-[4, 2, 2]
    filter_strdd = spark.sparkContext.parallelize(["Bills", "Mark", "Brain", "Mick"])
    filter_rdd1=filter_strdd.filter(lambda x:"B" in x).collect()
    print(filter_rdd1)#op-['Bills', 'Brain']

    # Sample function
    sample_rdd = spark.sparkContext.parallelize(range(1, 10))
    sample_rdd.collect()
    print(sample_rdd.sample(True,.2).collect())
    #op-[6, 6, 7, 9]

    #Union function
    num1=spark.sparkContext.parallelize([5,5,4,3,2,9,2])
    num2 = spark.sparkContext.parallelize([1,7,9,4,10,15])
    num3=num1.union(num2).collect()
    print(num3) #op-[5, 5, 4, 3, 2, 9, 2, 1, 7, 9, 4, 10, 15]



