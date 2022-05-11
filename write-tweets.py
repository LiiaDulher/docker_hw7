from kafka import KafkaProducer

import csv
import json
import time


class KafkaTweets:
    def __init__(self, kafka_host, topic_name, tweets_file):
        self.producer = KafkaProducer(bootstrap_servers=kafka_host,
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.topic = topic_name
        self.file = open(tweets_file, 'r')
        self.tweets = csv.reader(self.file)
        self.header = next(self.tweets)

    def write_tweets(self):
        k = 0
        while True:
            start_time = time.time()
            timestamp = time.strftime("%a, %b %d %H:%M:%S +0000 %Y", time.gmtime())
            data = next(self.tweets)
            tweet = {}
            for i in range(len(self.header)):
                tweet[self.header[i]] = data[i]
            tweet['created_at'] = timestamp
            self.producer.send(self.topic, tweet)
            k += 1
            if k == 10:
                time.sleep(start_time + 1 - time.time())
                k = 0

    def __del__(self):
        self.file.close()


def main():
    kafka_host_name = "kafka-server:9092"
    topic_name = "tweets"
    file_name = "/opt/app/twcs.csv"
    kafka_tweets = KafkaTweets(kafka_host_name, topic_name, file_name)
    kafka_tweets.write_tweets()


if __name__ == "__main__":
    main()
