from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [("Z", 1), ("A", 20), ("B", 30), ("C", 40), ("B", 30), ("B", 60)]
inputRDD = spark.sparkContext.parallelize(data)

listRdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 3, 2])

# fold
from operator import add

foldRes = listRdd.fold(0, add)
print(foldRes)  # output 20

# reduce
redRes = listRdd.reduce(add)
print(redRes)  # output 20

# Collect
data = listRdd.collect()
print(data)

# count, countApprox, countApproxDistinct
print("Count : " + str(listRdd.count()))
print(listRdd.count())
# Output: Count : 20

# countByValue, countByValueApprox
print("countByValue :  " + str(listRdd.countByValue()))
#countByValue :  defaultdict(<class 'int'>, {1: 1, 2: 2, 3: 2, 4: 1, 5: 1})

# first
print("first :  " + str(listRdd.first()))
# Output: first :  1
print("first :  " + str(inputRDD.first()))
# Output: first :  (Z,1)

# top
print("top : " + str(listRdd.top(2)))
# Output: take : 5,4
print("top : " + str(inputRDD.top(2)))
# Output: take : (Z,1),(C,40)

# min
print("min :  " + str(listRdd.min()))
# Output: min :  1
print("min :  " + str(inputRDD.min()))
# Output: min :  (A,20)

# max
print("max :  " + str(listRdd.max()))
# Output: max :  5
print("max :  " + str(inputRDD.max()))
# Output: max :  (Z,1)

# take
print("take : " + str(listRdd.take(2)))
# Output: take : 1,2
