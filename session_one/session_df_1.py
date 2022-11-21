import pyspark
from pyspark.sql import SparkSession
spark=SparkSession.builder.master("local[*]").appName("Dataframe session one").getOrCreate()

data=[("Ram","Male"),("Sham","Male"),("Kiran","FeMale")]
header=["Name","Gender"]
#creating rdd from existing rdd
input_rdd=spark.sparkContext.parallelize(data)
#creating dataframe from rdd without column names
input_df=input_rdd.toDF()
#input_df.show()
#creating dataframe from rdd with column names
#input_df1=input_rdd.toDF(header)
#input_df1.show()
#input_df1.printSchema()

#CreateDataframe
input_df2=spark.createDataFrame(input_rdd,schema=header)
#input_df2.show()

#CreateDataframe with data
input_df2=spark.createDataFrame(data,schema=header)
#input_df2.show()

#creating Dataframe with inputfile

#csv_df=spark.read.csv(r"C:\Users\admin\PycharmProjects\sparksession\input_data\employee.csv")
csv_df=spark.read.csv(r"C:\Users\admin\PycharmProjects\sparksession\input_data\employee.csv",schema="id int,name string,salary int")

csv_df.show()
csv_df.printSchema()