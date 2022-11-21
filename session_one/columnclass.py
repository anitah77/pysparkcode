from pyspark.sql import SparkSession
from  pyspark.sql import Row
from pyspark.sql.functions import col
if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("column class").getOrCreate()

    input_data = [Row(id=1, name="Ram", address=Row(city="Pune", state="MH")),
                  Row(id=2, name="Ram", address=Row(city="Pune", state="MH"))]

    df=spark.createDataFrame(input_data)
    df.show()
    df.select("id").show()
    df.select(df.address.city).show()
    df.select(df["address.city"]).show()
    df.select(col("address.city")).show()


    df.select(col("address.*")).show()