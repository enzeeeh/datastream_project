from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType
import xgboost as xgb
import pickle
import pandas as pd
from pyspark.sql.functions import udf

model_path = '/spark/model/best_xgb.pkl'
model = pickle.load(open(model_path, 'rb'))

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

spark = SparkSession.builder.appName("KafkaXGBoostStreaming").getOrCreate()

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

# Convert timestamp and numerical fields
df = df.withColumn("timestamp", to_timestamp(
    "timestamp", "yyyy-MM-dd HH:mm:ss"))
numeric_fields = [
    'P1_FCV01D', 'P1_FCV01Z', 'P1_FCV02D', 'P1_FCV02Z', 'P1_FCV03D', 'P1_FCV03Z',
    'P1_FT01', 'P1_FT01Z', 'P1_FT02', 'P1_FT02Z', 'P1_FT03', 'P1_FT03Z',
    'P1_LCV01D', 'P1_LCV01Z', 'P1_LIT01', 'P1_PCV01D', 'P1_PCV01Z',
    'P1_PCV02D', 'P1_PCV02Z', 'P1_PIT01', 'P1_PIT01_HH', 'P1_PIT02',
    'P1_PP01AD', 'P1_PP01AR', 'P1_PP01BD', 'P1_PP01BR', 'P1_PP02D', 'P1_PP02R',
    'P1_PP04', 'P1_PP04D', 'P1_PP04SP', 'P1_SOL01D', 'P1_SOL03D', 'P1_STSP',
    'P1_TIT01', 'P1_TIT02', 'P1_TIT03', 'P2_24Vdc', 'P2_ATSW_Lamp', 'P2_AutoGO',
    'P2_AutoSD', 'P2_Emerg', 'P2_MASW', 'P2_MASW_Lamp', 'P2_ManualGO',
    'P2_ManualSD', 'P2_OnOff', 'P2_RTR', 'P2_SCO', 'P2_SCST', 'P2_SIT01',
    'P2_TripEx', 'P2_VIBTR01', 'P2_VIBTR02', 'P2_VIBTR03', 'P2_VIBTR04',
    'P2_VT01', 'P2_VTR01', 'P2_VTR02', 'P2_VTR03', 'P2_VTR04', 'P3_FIT01',
    'P3_LCP01D', 'P3_LCV01D', 'P3_LH01', 'P3_LIT01', 'P3_LL01', 'P3_PIT01',
    'P4_HT_FD', 'P4_HT_PO', 'P4_HT_PS', 'P4_LD', 'P4_ST_FD', 'P4_ST_GOV',
    'P4_ST_LD', 'P4_ST_PO', 'P4_ST_PS', 'P4_ST_PT01', 'P4_ST_TT01',
    'x1001_05_SETPOINT_OUT', 'x1001_15_ASSIGN_OUT', 'x1002_07_SETPOINT_OUT',
    'x1002_08_SETPOINT_OUT', 'x1003_10_SETPOINT_OUT', 'x1003_18_SETPOINT_OUT',
    'x1003_24_SUM_OUT'
]

for field in numeric_fields:
    df = df.withColumn(field, col(field).cast("double"))

# Define the classification UDF


def classify(features):
    try:
        pandas_df = pd.DataFrame([features.asDict()])
        pandas_df = pandas_df.drop('timestamp', axis=1)
        pandas_df = pandas_df.apply(pd.to_numeric)
        dmatrix = xgb.DMatrix(pandas_df)
        prediction = model.predict(dmatrix)
        return 'Anomaly' if prediction[0] == 1 else 'Normal'
    except Exception as e:
        print(f"Error in classify UDF: {e}")
        return "Error"


classify_udf = udf(classify, StringType())
# # Apply the classification model to the DataFrame
result_df = df.withColumn("classification", classify_udf(
    struct([df[x] for x in df.columns])))


output_topic = 'classification'

# row_df = df.select(
#     to_json(struct("classification")).alias(
#         'key'),
#     to_json(struct(*[col for col in df.columns])
#             ).alias("value")
# )


query = result_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()


# query = row_df \
#     .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:29092") \
#     .option("topic", output_topic) \
#     .option("checkpointLocation", "/spark/checkpoint") \
#     .start()

# query.awaitTermination()
