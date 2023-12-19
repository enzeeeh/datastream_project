from kafka import KafkaProducer
import json
from csv import DictReader
import time
from confluent_kafka.admin import AdminClient, NewTopic

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'hai-security'
num_partitions = 1
replication_factor = 1

# Create AdminClient
admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})

# Define topic configuration
topic_config = {
    # Optional: customize additional topic-level configurations
    'cleanup.policy': 'delete',
}

# Create a NewTopic instance
new_topic = NewTopic(topic_name, num_partitions=num_partitions,
                     replication_factor=replication_factor, config=topic_config)
processedtopic = NewTopic('classification', num_partitions=num_partitions,
                          replication_factor=replication_factor, config=topic_config)

# Create the topic
try:
    admin_client.create_topics([new_topic])
    admin_client.create_topics([processedtopic])
    print(f"Topic '{topic_name}' created successfully")
    print(f"Topic 'classification' created successfully")

except Exception as e:
    print(f"Failed to create topic '{topic_name}': {e}")

# Initialize KafkaProducer
producer = KafkaProducer(bootstrap_servers=bootstrap_servers)


def on_send_success(record_metadata):
    print("Message sent to topic:", record_metadata.topic)
    print("Partition:", record_metadata.partition)
    print("Offset:", record_metadata.offset)


def on_send_error(excp):
    print('Error:', excp)


with open('./dataset/hai-test1.csv', 'r') as new_obj:
    csv_dict_reader = DictReader(new_obj)
    index = 0
    for row in csv_dict_reader:
        print('Trying to send:', row)
        future = producer.send(topic_name, json.dumps(row).encode('utf-8'))
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)

        index += 1
        if (index % 20) == 0:
            time.sleep(10)

producer.flush()
producer.close()
