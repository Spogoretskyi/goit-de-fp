from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    isnan,
    avg,
    current_timestamp,
    from_json,
    broadcast,
    when,
    struct,
    to_json,
    regexp_replace,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from configs import jdbc_config, spark_config, kafka_config
from schema import get_schema

import time
import random
import json
import os
import shutil

schema = get_schema()

spark_name = spark_config["name"]
spark_jar = spark_config["jar"]
spark_connector = spark_config["connector"]

jdbc_url = jdbc_config["url"]
jdbc_user = jdbc_config["user"]
jdbc_password = jdbc_config["password"]
jdbc_driver = jdbc_config["driver"]
jdbc_format = jdbc_config["format"]
table_bio = jdbc_config["table_bio"]
jdbc_table_events = jdbc_config["table_events"]
table_athlete_avg = jdbc_config["table_athlete_avg"]

kafka_format = kafka_config["format"]
kafka_servers = kafka_config["server_name"]
kafka_server_value = kafka_config["server_value"]
kafka_user = kafka_config["username"]
kafka_password = kafka_config["password"]
kafka_security_protocol = kafka_config["security_protocol"]
kafka_sasl_mechanism = kafka_config["sasl_mechanism"]
kafka_jaas_config = kafka_config["jaas_config"]


def clean_checkpoint_directory(checkpoint_dirs):
    for checkpoint_dir in checkpoint_dirs:
        if os.path.exists(checkpoint_dir):
            print(f"Cleaning checkpoint directory: {checkpoint_dir}")
            shutil.rmtree(checkpoint_dir)
        else:
            print(f"Checkpoint directory does not exist: {checkpoint_dir}")


def get_kafka_admin_client():
    return KafkaAdminClient(
        bootstrap_servers=kafka_server_value,
        security_protocol=kafka_security_protocol,
        sasl_mechanism=kafka_sasl_mechanism,
        sasl_plain_username=kafka_user,
        sasl_plain_password=kafka_password,
    )


def get_spark():
    return (
        SparkSession.builder.config(spark_jar, spark_connector)
        .appName(spark_name)
        .getOrCreate()
    )


def create_topic(client, topic):
    if not topic:
        print(f"Error: Missing 'name' for topic '{topic_id}'. Skipping.")
        return

    new_topic = NewTopic(
        name=topic,
        num_partitions=2,
        replication_factor=1,
    )

    try:
        client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"An error occurred while creating topic '{topic}': {e}")


### 1. Зчитування даних з таблиці athlete_bio
def select_from_athlete_bio(spark):
    return (
        spark.read.format(jdbc_format)
        .options(
            url=jdbc_url,
            driver=jdbc_driver,
            dbtable=table_bio,
            user=jdbc_user,
            password=jdbc_password,
        )
        .load()
    )


## 2. Фільтрація даних, де зрост і вага є порожніми або не є числами
def filter_athlete_bio(athlete_bio_df):
    return athlete_bio_df.filter(
        (col("height").isNotNull())
        & (col("weight").isNotNull())
        & (~isnan(col("height")))
        & (~isnan(col("weight")))
    )


### 3.1 Зчитування даних з таблиці athlete_event_results
def select_from_athlete_event_results(spark):
    return (
        spark.read.format(jdbc_format)
        .options(
            url=jdbc_url,
            driver=jdbc_driver,
            dbtable=jdbc_table_events,
            user=jdbc_user,
            password=jdbc_password,
        )
        .load()
    )


### Запис даних в Kafka топік
def write_to_kafka(df, topic):
    try:
        (
            df.selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value")
            .write.format(kafka_format)
            .option(kafka_servers, kafka_server_value)
            .option("kafka.security.protocol", kafka_security_protocol)
            .option("kafka.sasl.mechanism", kafka_sasl_mechanism)
            .option(
                kafka_jaas_config,
                f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_user}" password="{kafka_password}";',
            )
            .option("topic", topic)
            .save()
        )
    except Exception as e:
        print(f"Error writing to Kafka topic {topic}: {str(e)}")
        raise


# Зчитування даних з Kafka-топіку
def select_from_kafka(spark, topic):
    return (
        spark.readStream.format(kafka_format)
        .option(kafka_servers, kafka_server_value)
        .option("kafka.security.protocol", kafka_security_protocol)
        .option("kafka.sasl.mechanism", kafka_sasl_mechanism)
        .option(
            kafka_jaas_config,
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_user}" password="{kafka_password}";',
        )
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "5")
        .option("failOnDataLoss", "false")
        .load()
        .withColumn("value", regexp_replace(col("value").cast("string"), "\\\\", ""))
        .withColumn("value", regexp_replace(col("value"), '^"|"$', ""))
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.athlete_id", "data.sport", "data.medal")
    )


### 4 Об’єднання даних з результатами змагань з біологічними даними
def combine_data(filtered_athlete_bio_df, kafka_df):
    return kafka_df.join(
        filtered_athlete_bio_df,
        "athlete_id",
        "inner",
    ).select(
        filtered_athlete_bio_df["athlete_id"],
        filtered_athlete_bio_df["height"],
        filtered_athlete_bio_df["weight"],
        filtered_athlete_bio_df["sex"],
        filtered_athlete_bio_df["country_noc"].alias("bio_country_noc"),
        kafka_df["sport"],
        kafka_df["medal"],
        current_timestamp().alias("timestamp"),
    )


### 5. Обчислення середнього зросту та ваги для кожного виду спорту, типу медалі, статі та країни
def calculate_avg_bio(combined_df):
    return combined_df.groupBy("sport", "medal", "sex", "bio_country_noc").agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        current_timestamp().alias("timestamp"),
    )


### Запис результатів в базу даних
def write_batch_to_db(batch_df):
    batch_df.write.format(jdbc_format).option("url", jdbc_url).option(
        "driver", jdbc_driver
    ).option("dbtable", table_athlete_avg).option("user", jdbc_user).option(
        "password", jdbc_password
    ).mode(
        "append"
    ).save()


### Запис результатів в Kafka топік та базу даних
def foreach_batch_function(batch_df, topic):
    try:
        write_batch_to_db(batch_df)
        write_to_kafka(batch_df, topic)
    except Exception as e:
        print(f"Error in foreach_batch_function: {e}")


def batch_streaming(df, topic, checkpoint_dir):
    (
        df.writeStream.outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 50)
        .start()
    )

    (
        df.writeStream.outputMode("update")
        .foreachBatch(lambda batch_df, _: foreach_batch_function(batch_df, topic))
        .option("checkpointLocation", checkpoint_dir)
        .start()
        .awaitTermination()
    )
