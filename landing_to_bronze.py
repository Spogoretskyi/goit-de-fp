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

    # ftp.goit.study - не працюэ завантажив дані із таблиці
    # files = {
    #     "athlete_bio": "https://ftp.goit.study/neoversity/athlete_bio.txt",
    #     "athlete_event_results": "https://ftp.goit.study/neoversity/athlete_event_results.txt",
    # }

    files = {
        "athlete_bio": "tables/athlete_bio_202412222304.csv",
        "athlete_event_results": "tables/athlete_event_results_202412222309.csv",
    }

    # for table, url in files.items():
    #     csv_path = os.path.join(data_dir, f"{table}.csv")
    #     parquet_path = os.path.join(bronze_dir, table)

    #     download_file(url, csv_path)

    #     df = spark.read.option("header", "true").csv(csv_path)
    #     df.write.mode("overwrite").parquet(parquet_path)

    for table, csv_path in files.items():
        if not os.path.exists(csv_path):
            continue

        parquet_path = os.path.join(bronze_dir, table)
        df = spark.read.option("header", "true").csv(csv_path)
        df.show(truncate=False)
        df.write.mode("overwrite").parquet(parquet_path)

    print("*****Processed--> landing to bronze******")

if __name__ == "__main__":
    main()
