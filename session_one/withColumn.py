from pyspark.sql import SparkSession,column
from pyspark.sql.functions import col,lit
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DoubleType
if __name__ == '__main__':
    spark=SparkSession.builder.appName("With Coloumn").getOrCreate()

    schema_data=StructType([StructField("id",IntegerType()),
                            StructField("name",StringType()),
                            StructField("gender",StringType()),
                            StructField("city",StringType()),
                            StructField("salary",DoubleType())
                               ])
    df = spark.read.load(r"C:\Users\admin\PycharmProjects\sparksession\input_data\emp_withcol.csv",format="csv",schema=schema_data)

    df.printSchema()
    df.show()

    #change datatype of column(chaning datatype of salary)
    df1=df.withColumn("salary",col("salary").cast("Integer"))
    #df1.show()
    #df1.printSchema()

    #Increasing salary 100%
    df.withColumn("salary",col("salary")*100).show()
    #Adding column
    df.withColumn("bonus_percent", lit("Maharashtra")) \
        .show()
    #Adding col from exiting column
    df.withColumn("bonus_amount", df.salary * 0.3) \
        .show()

    #Rename column
    df.withColumn("name","first_name").printSchema()
    #Drop column
    df.drop("city").printSchema()