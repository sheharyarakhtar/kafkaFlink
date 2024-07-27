from kafka import KafkaProducer, KafkaConsumer
import requests as rq
from bs4 import BeautifulSoup
import os
import time
import json

os.environ['NEWS_API'] = '7618e996973244b8ab7c6812fa9fb182'

from newsapi import NewsApiClient

newsapi = NewsApiClient(api_key = os.environ.get('NEWS_API'))

# Kafka Producer Configuration
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_news_articles(topic, category):
    result = newsapi.get_top_headlines(q=topic,
                                       category=category,
                                       language='en',
                                       country='us')
    
    articles = result.get('articles', [])
    news_data = []

    for article in articles:
        news_data.append({
            'title': article['title'],
            'summary': article['description'],
            'link': article['url'],
            'publishedAt': article['publishedAt']
        })

    return news_data

def send_to_kafka(news_data, topic):
    for article in news_data:
        print(f"Sending to kafka {topic}")
        producer.send(topic, article)
        producer.flush()
        print('sent')

def monitor_news(topics):
    seen_articles = set()
    
    while True:
        for topic, category, kafka_topic in topics:
            print(f'finding articles for {topic}')
            news_articles = fetch_news_articles(topic, category)
            
            new_articles = [article for article in news_articles if article['link'] not in seen_articles]
            
            if new_articles:
                send_to_kafka(new_articles, kafka_topic)
                for article in new_articles:
                    seen_articles.add(article['link'])
        time.sleep(60)

if __name__ == "__main__":
    topics = [
        ('AI', 'technology', 'news-topic-ai'),
        ('Bitcoin', 'business', 'news-topic-de'),
        ('Large Language Models', 'technology', 'news-topic-llm')
    ]
    monitor_news(topics)
    print("Monitoring news articles and sending to Kafka.")