from pyspark.sql import SparkSession
from functions import (
    clean_checkpoint_directory,
    get_kafka_admin_client,
    create_topic,
    select_from_athlete_bio,
    select_from_athlete_event_results,
    filter_athlete_bio,
    write_to_kafka,
    select_from_kafka,
    combine_data,
    calculate_avg_bio,
    get_spark,
    batch_streaming,
)
from configs import kafka_config
import os


def main():
    kafka_topic_athlete_event_results = kafka_config["topic"]
    kafka_topic_avg_bio = kafka_config["topic_avg_bio"]

    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
    )

    checkpoint_directories = {
        "/tmp/checkpoints-1",
        "/tmp/checkpoints-2",
        "/tmp/checkpoints-3",
    }
    clean_checkpoint_directory(checkpoint_directories)

    spark = get_spark()

    kafka_admin_client = get_kafka_admin_client()

    create_topic(kafka_admin_client, kafka_topic_athlete_event_results)
    create_topic(kafka_admin_client, kafka_topic_avg_bio)

    ### 1. Зчитування даних з таблиці athlete_bio
    athlete_bio_df = select_from_athlete_bio(spark)

    ## 2. Фільтрація даних, де зрост і вага є порожніми або не є числами
    filtered_athlete_bio_df = filter_athlete_bio(athlete_bio_df)

    ### 3.1 Зчитування даних з таблиці athlete_event_results
    event_results_df = select_from_athlete_event_results(spark)

    ### 3.2 Запис даних в Kafka топік
    write_to_kafka(event_results_df, kafka_topic_athlete_event_results)

    # Зчитування даних з результатами змагань з Kafka-топіку athlete_event_results
    kafka_df = select_from_kafka(spark, kafka_topic_athlete_event_results)

    ### 4 Об’єднання даних з результатами змагань з біологічними даними
    combined_df = combine_data(filtered_athlete_bio_df, kafka_df)

    ### 5. Обчислення середнього зросту та ваги для кожного виду спорту, типу медалі, статі та країни
    avg_bio_df = calculate_avg_bio(combined_df)

    ### 6.1 Запис результатів в Kafka топік
    ### 6.2 Запис результатів в базу даних
    batch_streaming(avg_bio_df, kafka_topic_avg_bio, "/tmp/checkpoints-1")


if __name__ == "__main__":
    main()
