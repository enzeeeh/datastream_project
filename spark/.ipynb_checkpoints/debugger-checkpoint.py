from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType
import pickle

# Define your schema here
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("P1_FCV01D", StringType(), True),
    StructField("P1_FCV01Z", StringType(), True),
    StructField("P1_FCV02D", StringType(), True),
    StructField("P1_FCV02Z", StringType(), True),
    StructField("P1_FCV03D", StringType(), True),
    StructField("P1_FCV03Z", StringType(), True),
    StructField("P1_FT01", StringType(), True),
    StructField("P1_FT01Z", StringType(), True),
    StructField("P1_FT02", StringType(), True),
    StructField("P1_FT02Z", StringType(), True),
    StructField("P1_FT03", StringType(), True),
    StructField("P1_FT03Z", StringType(), True),
    StructField("P1_LCV01D", StringType(), True),
    StructField("P1_LCV01Z", StringType(), True),
    StructField("P1_LIT01", StringType(), True),
    StructField("P1_PCV01D", StringType(), True),
    StructField("P1_PCV01Z", StringType(), True),
    StructField("P1_PCV02D", StringType(), True),
    StructField("P1_PCV02Z", StringType(), True),
    StructField("P1_PIT01", StringType(), True),
    StructField("P1_PIT01_HH", StringType(), True),
    StructField("P1_PIT02", StringType(), True),
    StructField("P1_PP01AD", StringType(), True),
    StructField("P1_PP01AR", StringType(), True),
    StructField("P1_PP01BD", StringType(), True),
    StructField("P1_PP01BR", StringType(), True),
    StructField("P1_PP02D", StringType(), True),
    StructField("P1_PP02R", StringType(), True),
    StructField("P1_PP04", StringType(), True),
    StructField("P1_PP04D", StringType(), True),
    StructField("P1_PP04SP", StringType(), True),
    StructField("P1_SOL01D", StringType(), True),
    StructField("P1_SOL03D", StringType(), True),
    StructField("P1_STSP", StringType(), True),
    StructField("P1_TIT01", StringType(), True),
    StructField("P1_TIT02", StringType(), True),
    StructField("P1_TIT03", StringType(), True),
    StructField("P2_24Vdc", StringType(), True),
    StructField("P2_ATSW_Lamp", StringType(), True),
    StructField("P2_AutoGO", StringType(), True),
    StructField("P2_AutoSD", StringType(), True),
    StructField("P2_Emerg", StringType(), True),
    StructField("P2_MASW", StringType(), True),
    StructField("P2_MASW_Lamp", StringType(), True),
    StructField("P2_ManualGO", StringType(), True),
    StructField("P2_ManualSD", StringType(), True),
    StructField("P2_OnOff", StringType(), True),
    StructField("P2_RTR", StringType(), True),
    StructField("P2_SCO", StringType(), True),
    StructField("P2_SCST", StringType(), True),
    StructField("P2_SIT01", StringType(), True),
    StructField("P2_TripEx", StringType(), True),
    StructField("P2_VIBTR01", StringType(), True),
    StructField("P2_VIBTR02", StringType(), True),
    StructField("P2_VIBTR03", StringType(), True),
    StructField("P2_VIBTR04", StringType(), True),
    StructField("P2_VT01", StringType(), True),
    StructField("P2_VTR01", StringType(), True),
    StructField("P2_VTR02", StringType(), True),
    StructField("P2_VTR03", StringType(), True),
    StructField("P2_VTR04", StringType(), True),
    StructField("P3_FIT01", StringType(), True),
    StructField("P3_LCP01D", StringType(), True),
    StructField("P3_LCV01D", StringType(), True),
    StructField("P3_LH01", StringType(), True),
    StructField("P3_LIT01", StringType(), True),
    StructField("P3_LL01", StringType(), True),
    StructField("P3_PIT01", StringType(), True),
    StructField("P4_HT_FD", StringType(), True),
    StructField("P4_HT_PO", StringType(), True),
    StructField("P4_HT_PS", StringType(), True),
    StructField("P4_LD", StringType(), True),
    StructField("P4_ST_FD", StringType(), True),
    StructField("P4_ST_GOV", StringType(), True),
    StructField("P4_ST_LD", StringType(), True),
    StructField("P4_ST_PO", StringType(), True),
    StructField("P4_ST_PS", StringType(), True),
    StructField("P4_ST_PT01", StringType(), True),
    StructField("P4_ST_TT01", StringType(), True),
    StructField("x1001_05_SETPOINT_OUT", StringType(), True),
    StructField("x1001_15_ASSIGN_OUT", StringType(), True),
    StructField("x1002_07_SETPOINT_OUT", StringType(), True),
    StructField("x1002_08_SETPOINT_OUT", StringType(), True),
    StructField("x1003_10_SETPOINT_OUT", StringType(), True),
    StructField("x1003_18_SETPOINT_OUT", StringType(), True),
    StructField("x1003_24_SUM_OUT", StringType(), True)
])

# Initialize Spark Session
spark = SparkSession.builder.appName("KafkaStreaming").getOrCreate()

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "hai-security") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

# Convert JSON strings to DataFrame with schema
df = df.select(from_json(col("value"), schema).alias("data"))
df = df.select("data.*")

# Load the XGBoost model
model_path = '/spark/model/best_xgb.pkl'
model = pickle.load(open(model_path, 'rb'))

# Print streaming data to console for debugging
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
