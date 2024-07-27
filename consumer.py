from kafka import KafkaConsumer
import json

# Kafka Consumer Configuration
consumer = KafkaConsumer(
    'news-topic-ai', 'news-topic-de', 'news-topic-llm', 'news-topic-politics',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='news-group'
)

def process_message(message):
    topic = message.topic
    value = message.value
    
    print(f"Received message from topic '{topic}': {value}")
    
    # Add your processing logic here
    # For example, you can process and store the messages based on the topic
    if topic == 'news-topic-ai':
        print(topic)
        print(value)
        pass
    elif topic == 'news-topic-bitcoin':
        print(topic)
        print(value)
    elif topic == 'news-topic-llm':
        print(topic)
        print(value)
    elif topic == 'news-topic-politics':
        print(topic)
        print(value)

if __name__ == "__main__":
    for message in consumer:
        process_message(message)
