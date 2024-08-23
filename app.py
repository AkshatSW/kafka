from confluent_kafka import Producer, Consumer
import json
import os

# Replace with the actual SSL certificate path if needed (optional)
ssl_ca_location = None  # Path to your Kafka cluster's SSL certificate (optional)

# WARNING: Hardcoded credentials below are for development only.
# Do not commit this code to a public repository or use it in production!
bootstrap_servers = "SASL_SSL://ec2-3-208-232-40.compute-1.amazonaws.com:9096,SASL_SSL://ec2-100-24-239-47.compute-1.amazonaws.com:9096,SASL_SSL://ec2-3-220-121-33.compute-1.amazonaws.com:9096,SASL_SSL://ec2-100-25-107-37.compute-1.amazonaws.com:9096,SASL_SSL://ec2-3-231-110-127.compute-1.amazonaws.com:9096,SASL_SSL://ec2-3-227-129-191.compute-1.amazonaws.com:9096,SASL_SSL://ec2-35-168-34-44.compute-1.amazonaws.com:9096,SASL_SSL://ec2-3-234-75-100.compute-1.amazonaws.com:9096"

topic = "sensor_data"

producer_config = {
    "bootstrap.servers": bootstrap_servers,  # Set bootstrap servers
    "security.protocol": "SASL_SSL",  # Use SASL_SSL for SSL encryption
    "sasl.mechanisms": "SCRAM-SHA-512",
    "sasl.username": "akshatsw",
    "sasl.password": "12345678",
    # Optional: Provide path to SSL certificate if your cluster requires it
    "ssl.ca.location": ssl_ca_location,
}

consumer_config = {
    "bootstrap.servers": bootstrap_servers,  # Set bootstrap servers
    "security.protocol": "SASL_SSL",  # Use SASL_SSL for SSL encryption
    "sasl.mechanisms": "SCRAM-SHA-512",
    "sasl.username": "akshatsw",
    "sasl.password": "12345678",
    "group.id": "sensor-data-consumer",  # Identify consumer group
    "auto.offset.reset": "earliest",  # Start consuming from the beginning
}


def produce_sensor_data(data):
    producer = Producer(producer_config)
    producer.produce(topic, json.dumps(data))
    producer.flush()  # Wait for data to be delivered


def consume_sensor_data():
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])

    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        elif msg.error():
            print(f"Consumer error: {msg.error()}")
        else:
            print(f"Received sensor data: {json.loads(msg.value().decode('utf-8'))}")

        consumer.commit(autocommit=False)  # Manually commit offsets


if __name__ == "__main__":
    sensor_data = {"temperature": 25.5, "humidity": 60}
    produce_sensor_data(sensor_data)

    # Run the consumer in a separate thread (optional)
    # from threading import Thread
    # consumer_thread = Thread(target=consume_sensor_data)
    # consumer_thread.start()

    print(f"Produced sensor data: {sensor_data}")

    # Uncomment the following line to keep the main thread alive
    # while True:
    #     pass
