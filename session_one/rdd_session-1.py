import pyspark
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark=SparkSession.builder.master("local[*]").appName("RDD Test").getOrCreate()

    df = spark.read.text(r"C:\Users\admin\PycharmProjects\sparksession\input_data\emp_word.csv")

    df.selectExpr("split(value, ' ') as Text_Data_In_Rows_Using_Text").show(4, False)

    print('Number of lines in file : {}'.format(df.count()))
    # df.map(lambda s: len(s)).takeSample(True, 15)
    #df.flatMap(lambda line: line.split(" "))


