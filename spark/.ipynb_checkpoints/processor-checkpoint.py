from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pandas as pd
from pyspark.sql.functions import udf
import numpy as np
import pickle

import os
from os.path import abspath

# Set Environment
os.environ[
    "PYSPARK_SUBMIT_ARGS"
] = "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.0, org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell"

# Spark details
CHECKPOINT_PATH = "/home/jovyan/work/checkpoint"

# Kafka details
MODEL_PATH = "/home/jovyan/work/model/best_xgb.pkl"  # XGBoost model
KAFKA_SERVER = "kafka:29092"
KAFKA_INPUT_TOPIC = "hai-security"
KAFKA_OUTPUT_TOPIC = "classification"

# Create a Spark Session
spark = SparkSession.builder.appName("KafkaXGBoostStreaming").getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 100)

# Create a DataFrame that reads from the input Kafka topic name src-topic
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("subscribe", KAFKA_INPUT_TOPIC)
    .load()
    .selectExpr("CAST(value AS STRING)")
)

# Define columns
columns = [
    "timestamp",
    "P1_FCV01D",
    "P1_FCV01Z",
    "P1_FCV02D",
    "P1_FCV02Z",
    "P1_FCV03D",
    "P1_FCV03Z",
    "P1_FT01",
    "P1_FT01Z",
    "P1_FT02",
    "P1_FT02Z",
    "P1_FT03",
    "P1_FT03Z",
    "P1_LCV01D",
    "P1_LCV01Z",
    "P1_LIT01",
    "P1_PCV01D",
    "P1_PCV01Z",
    "P1_PCV02D",
    "P1_PCV02Z",
    "P1_PIT01",
    "P1_PIT01_HH",
    "P1_PIT02",
    "P1_PP01AD",
    "P1_PP01AR",
    "P1_PP01BD",
    "P1_PP01BR",
    "P1_PP02D",
    "P1_PP02R",
    "P1_PP04",
    "P1_PP04D",
    "P1_PP04SP",
    "P1_SOL01D",
    "P1_SOL03D",
    "P1_STSP",
    "P1_TIT01",
    "P1_TIT02",
    "P1_TIT03",
    "P2_24Vdc",
    "P2_ATSW_Lamp",
    "P2_AutoGO",
    "P2_AutoSD",
    "P2_Emerg",
    "P2_MASW",
    "P2_MASW_Lamp",
    "P2_ManualGO",
    "P2_ManualSD",
    "P2_OnOff",
    "P2_RTR",
    "P2_SCO",
    "P2_SCST",
    "P2_SIT01",
    "P2_TripEx",
    "P2_VIBTR01",
    "P2_VIBTR02",
    "P2_VIBTR03",
    "P2_VIBTR04",
    "P2_VT01",
    "P2_VTR01",
    "P2_VTR02",
    "P2_VTR03",
    "P2_VTR04",
    "P3_FIT01",
    "P3_LCP01D",
    "P3_LCV01D",
    "P3_LH01",
    "P3_LIT01",
    "P3_LL01",
    "P3_PIT01",
    "P4_HT_FD",
    "P4_HT_PO",
    "P4_HT_PS",
    "P4_LD",
    "P4_ST_FD",
    "P4_ST_GOV",
    "P4_ST_LD",
    "P4_ST_PO",
    "P4_ST_PS",
    "P4_ST_PT01",
    "P4_ST_TT01",
    "x1001_05_SETPOINT_OUT",
    "x1001_15_ASSIGN_OUT",
    "x1002_07_SETPOINT_OUT",
    "x1002_08_SETPOINT_OUT",
    "x1003_10_SETPOINT_OUT",
    "x1003_18_SETPOINT_OUT",
    "x1003_24_SUM_OUT",
]

# Create Schema
schema = StructType([StructField(col, StringType(), True) for col in columns])

# Convert JSON strings to DataFrame with schema
df = df.select(from_json(col("value"), schema).alias("data"))
df = df.select("data.*")

# Convert numeric to double
numeric_fields = columns[1:]
for field in numeric_fields:
    df = df.withColumn(field, col(field).cast(DoubleType()))

# Clasification Process
model = pickle.load(open(MODEL_PATH, "rb"))


# Model Prediction
def classify(features):
    try:
        features_dict = features.asDict()
        pandas_df = pd.DataFrame([features_dict])
        pandas_df = pandas_df.apply(pd.to_numeric, errors="ignore")
        pandas_df = pandas_df.select_dtypes(include=[np.number])

        # Directly use pandas_df for prediction
        prediction = model.predict(pandas_df)
        print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
        print(prediction[0])
        print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX")

        return 'Anomaly' if prediction[0] == 1 else 'Normal'

    except Exception as e:
        print(f"Error in classify UDF at step [{e.__traceback__.tb_lineno}]: {e}")
        return e


# User defined function
classify_udf = udf(classify, StringType())

result_df = df.withColumn(
    "classification",
    classify_udf(struct([df[x] for x in df.columns if x != "timestamp"])),
)

# Show the classified data to terminal
# query = result_df.writeStream.outputMode("append").format("console").start()

# query.awaitTermination()

# Write the classified data to another Kafka topic
stream_query = (
    result_df.selectExpr("to_json(struct(*)) AS value")
    .writeStream.format("kafka")
    .outputMode("append")
    .option("kafka.bootstrap.servers", KAFKA_SERVER)
    .option("topic", KAFKA_OUTPUT_TOPIC)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)

stream_query.awaitTermination()
