from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
import os


def clean_text_columns(df):
    for column in df.columns:
        if df.schema[column].dataType.simpleString() == "string":
            df = df.withColumn(column, trim(col(column)))
    return df


def main():
    spark = SparkSession.builder.appName("BronzeToSilver").getOrCreate()

    bronze_dir = "data/bronze"
    silver_dir = "data/silver"
    os.makedirs(silver_dir, exist_ok=True)

    tables = ["athlete_bio", "athlete_event_results"]

    for table in tables:
        bronze_path = os.path.join(bronze_dir, table)
        silver_path = os.path.join(silver_dir, table)

        df = spark.read.parquet(bronze_path)

        df_cleaned = clean_text_columns(df).dropDuplicates()

        df_cleaned.show(truncate=False)

        df_cleaned.write.mode("overwrite").parquet(silver_path)

    print("*****Processed--> bronze to silver******")


if __name__ == "__main__":
    main()
