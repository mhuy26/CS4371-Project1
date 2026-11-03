"""Train the required Spark MLlib AG News classification pipeline."""
import argparse
from pathlib import Path
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.sql import functions as F
from part2_common import spark_session, CLASS_NAMES

p = argparse.ArgumentParser()
p.add_argument('--ag-news', required=True, help='Path to AG News train Parquet file or directory')
p.add_argument('--model', required=True, help='Destination for fitted PipelineModel')
p.add_argument('--features', type=int, default=1 << 16)
p.add_argument('--max-iter', type=int, default=30)
a = p.parse_args()
spark = spark_session('CS4371-AGNews-Train')
try:
    train = (spark.read.parquet(a.ag_news)
             .select(F.col('text').cast('string').alias('text'),
                     F.col('label').cast('double').alias('label'))
             .filter(F.col('text').isNotNull() & F.col('label').between(0, 3)))
    total = train.count()
    print('AG News training count:', total)
    train.groupBy('label').count().orderBy('label').show()
    if total != 120000:
        print('WARNING: assignment specifies the full 120,000-row AG News training split.')
    pipeline = Pipeline(stages=[
        RegexTokenizer(inputCol='text', outputCol='tokens', pattern='[^a-zA-Z]+', gaps=True,
                       toLowercase=True, minTokenLength=1),
        StopWordsRemover(inputCol='tokens', outputCol='filtered'),
        HashingTF(inputCol='filtered', outputCol='tf', numFeatures=a.features),
        IDF(inputCol='tf', outputCol='features', minDocFreq=2),
        LogisticRegression(featuresCol='features', labelCol='label', maxIter=a.max_iter,
                           regParam=0.1, family='multinomial'),
    ])
    fitted = pipeline.fit(train)
    fitted.write().overwrite().save(a.model)
    print('Saved model:', a.model)
    print('Labels:', ', '.join(f'{i}={v}' for i, v in enumerate(CLASS_NAMES)))
finally:
    spark.stop()
