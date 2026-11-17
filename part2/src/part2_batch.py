"""CS4371 Group Project 1 - Part 2: Batch text classification.

Loads the trained AG News model, classifies Hacker News messages,
and exports Google Sheets-compatible CSV files.
"""

import argparse

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Window
from pyspark.sql import functions as F

from part2_common import (
    spark_session,
    text_from_json_col,
    clean_text,
    names,
)


def write_csv(df, path):
    """Write CSV with standard double-quote escaping."""
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .option("quoteAll", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(path)
    )


def main():
    parser = argparse.ArgumentParser(
        description="Batch classify Hacker News messages using AG News"
    )

    parser.add_argument(
        "--input",
        nargs="+",
        required=True,
        help="One or more producer JSONL files",
    )

    parser.add_argument(
        "--model",
        required=True,
        help="Path to saved Spark PipelineModel",
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Output directory",
    )

    parser.add_argument(
        "--minimum",
        type=int,
        default=1000,
        help="Minimum number of raw messages",
    )

    args = parser.parse_args()

    spark = spark_session("CS4371-AGNews-Batch")

    try:
        # Load original Hacker News JSONL records.
        raw = spark.read.text(args.input)

        decoded = raw.select(
            text_from_json_col(F.col("value")).alias("text")
        )

        raw_count = decoded.count()

        # Remove empty and duplicate messages.
        cleaned = clean_text(decoded).cache()
        clean_count = cleaned.count()

        print(f"Raw messages: {raw_count}")
        print(f"Unique nonempty messages: {clean_count}")
        print(f"Removed: {raw_count - clean_count}")

        if raw_count < args.minimum:
            raise SystemExit(
                f"ERROR: {raw_count} < required "
                f"{args.minimum} streamed messages."
            )

        # Load the existing fitted model.
        model = PipelineModel.load(args.model)

        # Classify messages using the saved pipeline.
        predicted = names(model.transform(cleaned))

        result = (
            predicted
            .withColumn(
                "predicted_probability",
                F.element_at(
                    vector_to_array(F.col("probability")),
                    F.col("prediction").cast("int") + F.lit(1),
                ),
            )
            .select(
                "text",
                "predicted_class",
                "predicted_probability",
            )
            .cache()
        )

        print("\nPredicted class counts:")

        (
            result
            .groupBy("predicted_class")
            .count()
            .orderBy("predicted_class")
            .show(truncate=False)
        )

        # 1. Export predictions for every message.
        write_csv(
            result,
            f"{args.output}/all_predictions",
        )

        # 2. Select five random examples from each class.
        window = (
            Window
            .partitionBy("predicted_class")
            .orderBy(F.rand(seed=42))
        )

        examples = (
            result
            .withColumn("rank", F.row_number().over(window))
            .where(F.col("rank") <= 5)
            .drop("rank")
        )

        write_csv(
            examples,
            f"{args.output}/five_per_class",
        )

        # 3. Randomly select 50 messages for manual evaluation.
        sample = (
            result
            .orderBy(F.rand(seed=2026))
            .limit(50)
            .withColumn("manual_label", F.lit(""))
            .select(
                "text",
                "predicted_class",
                "predicted_probability",
                "manual_label",
            )
        )

        write_csv(
            sample,
            f"{args.output}/hand_label_50",
        )

        print(f"\nOutput directory: {args.output}")
        print("Created:")
        print("  - all_predictions/")
        print("  - five_per_class/")
        print("  - hand_label_50/")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
