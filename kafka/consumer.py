import datetime
from kafka import KafkaConsumer
import json
from confluent_kafka.admin import AdminClient
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Kafka configuration
bootstrap_servers = "localhost:9092"
topic_name = "classification"
num_partitions = 1
replication_factor = 1

# InfluxDB configuration
influxdb_url = "http://localhost:8086"
token = "9XkMEDhwP4qWHEb3Un3FETeXdtfGyipG1GJZLNI7yt07Na2bXfLWY4V-yjMiXLNfOWPGeNlplfph6DdV3NXcxQ=="
org = "securityDataset"
bucket = "securityDataset"

# Kafka client init
admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

# Define topic configuration
topic_config = {
    # Optional: customize additional topic-level configurations
    "cleanup.policy": "delete",
}

# Initialize KafkaConsumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="my-group",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print(f"Subscribed to '{topic_name}' and waiting for messages...")

# Dict of float columns
field_mapping = {
    "P1_FCV01D": "P1_FCV01D",
    "P1_FCV01Z": "P1_FCV01Z",
    "P1_FCV02D": "P1_FCV02D",
    "P1_FCV02Z": "P1_FCV02Z",
    "P1_FCV03D": "P1_FCV03D",
    "P1_FCV03Z": "P1_FCV03Z",
    "P1_FT01": "P1_FT01",
    "P1_FT01Z": "P1_FT01Z",
    "P1_FT02": "P1_FT02",
    "P1_FT02Z": "P1_FT02Z",
    "P1_FT03": "P1_FT03",
    "P1_FT03Z": "P1_FT03Z",
    "P1_LCV01D": "P1_LCV01D",
    "P1_LCV01Z": "P1_LCV01Z",
    "P1_LIT01": "P1_LIT01",
    "P1_PCV01D": "P1_PCV01D",
    "P1_PCV01Z": "P1_PCV01Z",
    "P1_PCV02D": "P1_PCV02D",
    "P1_PCV02Z": "P1_PCV02Z",
    "P1_PIT01": "P1_PIT01",
    "P1_PIT01_HH": "P1_PIT01_HH",
    "P1_PIT02": "P1_PIT02",
    "P1_PP01AD": "P1_PP01AD",
    "P1_PP01AR": "P1_PP01AR",
    "P1_PP01BD": "P1_PP01BD",
    "P1_PP01BR": "P1_PP01BR",
    "P1_PP02D": "P1_PP02D",
    "P1_PP02R": "P1_PP02R",
    "P1_PP04": "P1_PP04",
    "P1_PP04D": "P1_PP04D",
    "P1_PP04SP": "P1_PP04SP",
    "P1_SOL01D": "P1_SOL01D",
    "P1_SOL03D": "P1_SOL03D",
    "P1_STSP": "P1_STSP",
    "P1_TIT01": "P1_TIT01",
    "P1_TIT02": "P1_TIT02",
    "P1_TIT03": "P1_TIT03",
    "P2_24Vdc": "P2_24Vdc",
    "P2_ATSW_Lamp": "P2_ATSW_Lamp",
    "P2_AutoGO": "P2_AutoGO",
    "P2_AutoSD": "P2_AutoSD",
    "P2_Emerg": "P2_Emerg",
    "P2_MASW": "P2_MASW",
    "P2_MASW_Lamp": "P2_MASW_Lamp",
    "P2_ManualGO": "P2_ManualGO",
    "P2_ManualSD": "P2_ManualSD",
    "P2_OnOff": "P2_OnOff",
    "P2_RTR": "P2_RTR",
    "P2_SCO": "P2_SCO",
    "P2_SCST": "P2_SCST",
    "P2_SIT01": "P2_SIT01",
    "P2_TripEx": "P2_TripEx",
    "P2_VIBTR01": "P2_VIBTR01",
    "P2_VIBTR02": "P2_VIBTR02",
    "P2_VIBTR03": "P2_VIBTR03",
    "P2_VIBTR04": "P2_VIBTR04",
    "P2_VT01": "P2_VT01",
    "P2_VTR01": "P2_VTR01",
    "P2_VTR02": "P2_VTR02",
    "P2_VTR03": "P2_VTR03",
    "P2_VTR04": "P2_VTR04",
    "P3_FIT01": "P3_FIT01",
    "P3_LCP01D": "P3_LCP01D",
    "P3_LCV01D": "P3_LCV01D",
    "P3_LH01": "P3_LH01",
    "P3_LIT01": "P3_LIT01",
    "P3_LL01": "P3_LL01",
    "P3_PIT01": "P3_PIT01",
    "P4_HT_FD": "P4_HT_FD",
    "P4_HT_PO": "P4_HT_PO",
    "P4_HT_PS": "P4_HT_PS",
    "P4_LD": "P4_LD",
    "P4_ST_FD": "P4_ST_FD",
    "P4_ST_GOV": "P4_ST_GOV",
    "P4_ST_LD": "P4_ST_LD",
    "P4_ST_PO": "P4_ST_PO",
    "P4_ST_PS": "P4_ST_PS",
    "P4_ST_PT01": "P4_ST_PT01",
    "P4_ST_TT01": "P4_ST_TT01",
    "x1001_05_SETPOINT_OUT": "x1001_05_SETPOINT_OUT",
    "x1001_15_ASSIGN_OUT": "x1001_15_ASSIGN_OUT",
    "x1002_07_SETPOINT_OUT": "x1002_07_SETPOINT_OUT",
    "x1002_08_SETPOINT_OUT": "x1002_08_SETPOINT_OUT",
    "x1003_10_SETPOINT_OUT": "x1003_10_SETPOINT_OUT",
    "x1003_18_SETPOINT_OUT": "x1003_18_SETPOINT_OUT",
    "x1003_24_SUM_OUT": "x1003_24_SUM_OUT",
}
# Init client
client = InfluxDBClient(url=influxdb_url, token=token, org=org)

# Initialize SYNCHRONOUS instance of WriteApi
write_api = client.write_api(write_options=SYNCHRONOUS)

# Continuously listen for messages
for message in consumer:
    try:
        # Get detail from message
        mvalue = message.value
        mkey = message.key
        mpart = message.partition
        moffset = message.offset

        # Insert data to datapoint, point data structure to represent LineProtocol
        dataPoint = (
            Point("classification")
            .time(datetime.datetime.strptime(mvalue["timestamp"], "%Y-%m-%d %H:%M:%S"))
            .field("classification", str(mvalue["classification"]))
        )

        # Mapping float columns
        for point_column, data_column in field_mapping.items():
            dataPoint = dataPoint.field(point_column, float(mvalue[data_column]))

        print(dataPoint)

        print("Received message:")
        print("Topic:", message.topic)
        print("Partition:", message.partition)
        print("Offset:", message.offset)
        print("Key:", message.key)
        print("Value:", message.value)
        print("-" * 50)

        # Write time-series data into InfluxDB
        write_api.write(bucket=bucket, record=dataPoint)

        print("Writing was successful to classification topic")

    except Exception as e:
        print("Error: ", e)

# Close the InfluxDB client connection
client.close()