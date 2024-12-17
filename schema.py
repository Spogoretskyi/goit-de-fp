from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def get_schema():
    return StructType(
        [
            StructField("athlete_id", StringType(), True),
            StructField("sport", StringType(), True),
            StructField("medal", StringType(), True),
            StructField("timestamp", StringType(), True),
        ]
    )
