"""Common Spark helpers for CS4371 Group Project 1 Part 2."""
import os
from pyspark.sql import SparkSession, functions as F, types as T

CLASS_NAMES = ['World', 'Sports', 'Business', 'Sci/Tech']
PAYLOAD_SCHEMA = T.StructType([
    T.StructField('id', T.StringType()),
    T.StructField('text', T.StringType()),
    T.StructField('title', T.StringType()),
    T.StructField('body', T.StringType()),
    T.StructField('type', T.StringType()),
])


def spark_session(name, kafka=False):
    b = (SparkSession.builder.appName(name).master(os.environ.get('SPARK_MASTER', 'local[2]'))
         .config('spark.sql.shuffle.partitions', '2')
         .config('spark.driver.memory', os.environ.get('SPARK_DRIVER_MEMORY', '2g')))
    if kafka:
        b = b.config('spark.jars.packages', os.environ.get('SPARK_KAFKA_PACKAGE',
             'org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3'))
    return b.getOrCreate()


def text_from_json_col(raw):
    j = F.from_json(raw, PAYLOAD_SCHEMA)
    return F.coalesce(j['text'], j['body'], j['title'], F.when(j.isNull(), raw))


def clean_text(df):
    return (df.select(F.trim(F.col('text')).alias('text'))
            .where(F.col('text').isNotNull() & (F.length('text') > 0))
            .dropDuplicates(['text']))


def names(df):
    return df.withColumn('predicted_class', F.element_at(
        F.array(*[F.lit(v) for v in CLASS_NAMES]),
        (F.col('prediction').cast('int') + F.lit(1))))
