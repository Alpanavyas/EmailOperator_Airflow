# spark-submit \
# --master yarn \
# --deploy-mode client \
# --driver-memory 1g \
# --executor-memory 2g \
# --num-executors 1 \
# --executor-cores 4 \
# /home/saif/PycharmProjects/cohort_c8//assign_29/jan_06_2022.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import split,to_date,year,date_format,month
from pyspark.sql.types import StringType,StructType,StructField,ArrayType
from pyspark.sql.functions import col,array_contains,sum,avg,max,lit,explode
from pyspark.sql.window import Window

if __name__ == '__main__':
    spark = SparkSession.builder \
            .appName('pyspark 06/01/22') \
            .master('local[*]')\
            .getOrCreate()
    df = spark.read.format('csv') \
        .options(header='True', inferSchema='True') \
        .load('file:///home/alpana/LFS/datasets/txns')

    df1 = df.withColumn('txndate',to_date(col('txndate'),'mm-dd-yyyy'))

    df2 = df1.withColumn('year',year(col('txndate')))\
            .withColumn('month',month(col('txndate')))\
            .withColumn('day',date_format(col('txndate'),"EEEE"))

    df2.select("txnno","txndate","custno","amount","category","product","city","state",
               col('txndate'),col('year'),col('month'),col('day')).show(5)

    df3 = df2.groupBy(col('day')).agg(sum(col('amount')).cast('Integer').alias('sum'))
    df3.show()

    df3.write.format('json')\
        .mode('overwrite')\
        .save('hdfs://localhost:9000/user/alpana/HFS/Input/txn_json')
