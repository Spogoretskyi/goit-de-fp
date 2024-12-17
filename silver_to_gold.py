from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_timestamp
import os


def main():
    spark = SparkSession.builder.appName("SilverToGold").getOrCreate()

    silver_dir = "data/silver"
    gold_dir = "data/gold"
    os.makedirs(gold_dir, exist_ok=True)

    athlete_bio_path = os.path.join(silver_dir, "athlete_bio")
    athlete_event_results_path = os.path.join(silver_dir, "athlete_event_results")
    gold_path = os.path.join(gold_dir, "avg_stats")

    bio_df = spark.read.parquet(athlete_bio_path)
    event_results_df = spark.read.parquet(athlete_event_results_path)

    joined_df = bio_df.join(event_results_df, on="athlete_id")

    grouped_df = joined_df.groupBy("sport", "medal", "sex", "country_noc").agg(
        avg(col("weight")).alias("avg_weight"), avg(col("height")).alias("avg_height")
    )

    final_df = grouped_df.withColumn("timestamp", current_timestamp())

    final_df.write.mode("overwrite").parquet(gold_path)


if __name__ == "__main__":
    main()
