from kafka import KafkaConsumer
import json
from confluent_kafka.admin import AdminClient, NewTopic

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'classification'
num_partitions = 1
replication_factor = 1

admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

# Define topic configuration
topic_config = {
    # Optional: customize additional topic-level configurations
    'cleanup.policy': 'delete',
}
processedtopic = NewTopic('classification', num_partitions=num_partitions,
                          replication_factor=replication_factor, config=topic_config)

# Create the topic
try:
    admin_client.create_topics([processedtopic])
    print(f"Topic '{topic_name}' created successfully")

except Exception as e:
    print(f"Failed to create topic '{topic_name}': {e}")

# Initialize KafkaConsumer
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(
        x.decode('utf-8'))
)

print(f"Subscribed to '{topic_name}' and waiting for messages...")

# Continuously listen for messages
for message in consumer:
    print("Received message:")
    print("Topic:", message.topic)
    print("Partition:", message.partition)
    print("Offset:", message.offset)
    print("Key:", message.key)
    print("Value:", message.value)
    print("-" * 50)
