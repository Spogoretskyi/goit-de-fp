from pyspark.sql import SparkSession
import requests
import os


def download_file(url, save_path):
    response = requests.get(url)
    response.raise_for_status()
    with open(save_path, "wb") as f:
        f.write(response.content)


def main():
    spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()

    data_dir = "data/landing"
    bronze_dir = "data/bronze"
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(bronze_dir, exist_ok=True)

    files = {
        "athlete_bio": "https://ftp.goit.study/neoversity/athlete_bio.txt",
        "athlete_event_results": "https://ftp.goit.study/neoversity/athlete_event_results.txt",
    }

    for table, url in files.items():
        csv_path = os.path.join(data_dir, f"{table}.csv")
        parquet_path = os.path.join(bronze_dir, table)

        download_file(url, csv_path)

        df = spark.read.option("header", "true").csv(csv_path)
        df.write.mode("overwrite").parquet(parquet_path)


if __name__ == "__main__":
    main()
