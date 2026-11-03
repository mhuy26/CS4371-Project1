"""A SECOND Structured Streaming query to run alongside Part 1 NER."""
import argparse
from pyspark.ml import PipelineModel
from pyspark.sql import functions as F
from part2_common import spark_session, text_from_json_col, names

p = argparse.ArgumentParser()
p.add_argument('--model', required=True)
p.add_argument('--topic', required=True)
p.add_argument('--bootstrap', default='localhost:9092')
p.add_argument('--checkpoint', required=True)
p.add_argument('--trigger-seconds', type=int, default=30)
p.add_argument('--from-earliest', action='store_true')
a = p.parse_args()
spark = spark_session('CS4371-AGNews-Streaming', kafka=True)
spark.sparkContext.setLogLevel('WARN')
try:
    model = PipelineModel.load(a.model)
    source = (spark.readStream.format('kafka')
              .option('kafka.bootstrap.servers', a.bootstrap)
              .option('subscribe', a.topic)
              .option('startingOffsets', 'earliest' if a.from_earliest else 'latest')
              .load())
    texts = source.select(text_from_json_col(F.col('value').cast('string')).alias('text'))
    texts = texts.withColumn('text', F.trim('text')).where(F.col('text').isNotNull() & (F.length('text') > 0))
    transformed = names(model.transform(texts))
    running = transformed.groupBy('predicted_class').count()
    query = (running.writeStream.outputMode('complete').format('console')
             .option('truncate', 'false')
             .option('checkpointLocation', a.checkpoint)
             .trigger(processingTime=f'{a.trigger_seconds} seconds').start())
    print(f'Streaming classification on {a.topic}; Ctrl+C stops the query.')
    query.awaitTermination()
finally:
    spark.stop()
