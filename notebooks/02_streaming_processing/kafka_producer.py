from confluent_kafka import Producer
import time
import json
import random

# Lists of words for sentence components
nouns = ["dog", "cat", "car", "ball", "bird"]
verbs = ["runs", "jumps", "flies", "sings", "eats"]
adjectives = ["big", "small", "fast", "lazy", "colorful"]
adverbs = ["quickly", "slowly", "loudly", "gracefully", "hungrily"]

# Kafka producer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',  # Replace with your Kafka broker address
    'client.id': 'python-producer',
    'security.protocol': 'PLAINTEXT'  # Explicitly disable SSL
}

producer = Producer(conf)

# Function to generate a random sentence
def generate_random_sentence():
    # Simple sentence structure: [Adjective] [Noun] [Verb] [Adverb]
    sentence = f"The {random.choice(adjectives)} {random.choice(nouns)} {random.choice(verbs)} {random.choice(adverbs)}."
    return sentence

# Callback to handle delivery reports
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

# Produce messages
topic = "test-topic"
print(f"Producing messages to Kafka topic '{topic}'...")

try:
    for i in range(1000):  # Generate 10 random messages
        message_content = generate_random_sentence()
        message = {
            "message_id": i,
            "message_content": message_content
        }

        # Send message to Kafka
        producer.produce(topic, json.dumps(message), callback=delivery_report)
        print(f"Sent: {message}")
        producer.flush()
        time.sleep(1)  # Pause between messages

    # Wait for any outstanding messages to be delivered
    producer.flush()
finally:
    print("Closing producer.")
