jdbc_config = {
    "url": "jdbc:mysql://217.61.57.46:3306/olympic_dataset",
    "table_bio": "athlete_bio",
    "table_events": "athlete_event_results",
    "table_athlete_avg": "spogoretskyi_enriched_athlete_avg",
    "user": "neo_data_admin",
    "password": "Proyahaxuqithab9oplp",
    "format": "jdbc",
    "driver": "com.mysql.cj.jdbc.Driver",
    "jar": "spark.jars",
}

spark_config = {
    "name": "JDBCToKafka",
    "connector": "./connector/mysql-connector-j-8.0.32.jar",
    "jar": "spark.jars",
}

kafka_config = {
    "format": "kafka",
    "server_name": "kafka.bootstrap.servers",
    "server_value": "77.81.230.104:9092",
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "topic": "athlete_event_results",
    "topic_avg_bio": "spogoretskyi_enriched_athlete_avg",
    "jaas_config": "kafka.sasl.jaas.config",
}
