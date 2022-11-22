import pyspark
from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("RDD narrow Transformation").getOrCreate()


#num1 = spark.sparkContext.parallelize([5, 5, 4, 3, 2, 9, 2])
#num2 = spark.sparkContext.parallelize([1, 7, 9, 4, 10, 15])

#intersection function
#inter_num = num1.intersection(num2).collect()
#print(inter_num)

#subtract
#subtract_num = num1.subtract(num2).collect()
#print("substract result:"+subtract_num)

#Distinct
#dist_num=num1.distinct(num2).collect()
#print("Result of distinct:"+dist_num)

#countByvalue
#data=spark.sparkContext.parallelize([(1,2),(3,4),(3,6),(3,4)])

#count()
#print(data.count())
#print(data.countByValues())

#countBykey
#print(data.countByKey())
#top()
"""
rdd=spark.sparkContext.parallelize([10,4,2,12,3])
print(rdd.top(1))
print(rdd.top(2))
print(rdd.top(3,key=str))

#sorByKey
srdd=spark.sparkContext.parallelize([(1,2),(4,4),(3,6),(3,4)])
print(srdd.sortByKey().collect())
#lookup
print(srdd.lookup(3))
print(srdd.keys().collect())
print(srdd.values().collect())

#mapValues()
mdata=srdd.mapValues(lambda a:a*a).collect()
print(mdata)

#reduceBykey
rdata=srdd.reduceByKey(lambda x,y:x+y).collect()
rdata1=srdd.reduceByKey(max).collect()
print(rdata)
print(rdata1)

#groupByKey
gresult=data.groupByKey().collect()
for (k,v) in gresult:
    print(k,list(v))

#reduceBykey=groupbykey.mapvalues()
aa=data.groupByKey().mapValues(max).collect()
print(aa)

bb=data.groupByKey().mapValues(sum).collect()
print(bb)

#flatMapValues
fmv=spark.sparkContext.parallelize([(1,2),(3,4),(3,6),(3,4)])
fmv1=fmv.flatMapValues(lambda x:range(1,x)).collect()
print(fmv1)

#substractByKey
sbk1=spark.sparkContext.parallelize([(1,2),(3,4),(3,6),(3,4)])
sbk2=spark.sparkContext.parallelize([(3,9)])
sbk3=sbk1.subtractByKey(sbk2).collect()
sbk4=sbk2.subtractByKey(sbk1).collect()
print(sbk3)
print(sbk4)

#Join
data1=spark.sparkContext.parallelize([(1,2),(3,4),(3,6),(3,4)])
data2=spark.sparkContext.parallelize([(3,9),(4,15)])
jresult=data1.join(data2).collect()
print(jresult)

#RightOuterjoin
data1=spark.sparkContext.parallelize([(1,2),(3,4),(3,6),(3,4)])
data2=spark.sparkContext.parallelize([(3,9),(4,15)])
rjoin=data1.rightOuterJoin(data2).collect()
print(rjoin)

#LefttOuterjoin
data1=spark.sparkContext.parallelize([(1,2),(3,4),(3,6),(3,4)])
data2=spark.sparkContext.parallelize([(3,9),(4,15)])
ljoin=data1.leftOuterJoin(data2).collect()
print(ljoin)
"""
#fullOuterjoin
data1=spark.sparkContext.parallelize([(1,2),(3,4),(3,6),(3,4)])
data2=spark.sparkContext.parallelize([(3,9),(4,15)])
fjoin=data2.fullOuterJoin(data1).collect()
print(fjoin)





