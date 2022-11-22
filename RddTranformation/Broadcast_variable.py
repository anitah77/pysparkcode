import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Braodcast Variable").getOrCreate()

states={"MH":"Maharashtra","UP":"UttarPradesh"}
broadcaststates=spark.sparkContext.broadcast(states)

data=[(1,"Ram","xyz","MH"),(2,"Sham","pqr","MH"),(3,"Kiran","nmn","UP")]
rdd=spark.sparkContext.parallelize(data)
print(rdd.collect())
#fuction define for passing state code
def replacestatevalue(statecode):
    return  broadcaststates.value[statecode]

result = rdd.map(lambda x:(x[0],x[1],x[2],replacestatevalue(x[3]))).collect()
print(result)


