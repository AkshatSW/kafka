from confluent_kafka import Producer

# Replace placeholders with actual values from Heroku Kafka add-on info
cluster_url = "your_cluster_url:9092"
username = "your_username"
password = "your_password"

# Configure the Kafka producer with cluster details (updated security protocol)
producer = Producer({
    'bootstrap.servers': cluster_url,
    'security.protocol': 'SSL',  # Replace with the correct protocol from Heroku
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': username,
    'sasl.password': password
})

# Define the topic to publish events to
topic = "user-registrations"

# Simulate user registrations by publishing events
for i in range(10):
    producer.poll(0)  # Check for delivery reports
    data = {"user_id": i, "username": f"user{i}"}  # Sample user data
    producer.produce(topic, str(data).encode('utf-8'))  # Send data to Kafka
    producer.poll(1)  # Wait for delivery

producer.flush()  # Flush any remaining messages
