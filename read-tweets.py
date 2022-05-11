from kafka import KafkaConsumer, TopicPartition

import csv
import json
import time


class KafkaTweetsReader:
    def __init__(self, kafka_host, topic_name):
        self.consumer = KafkaConsumer(bootstrap_servers=kafka_host)
        self.consumer.assign([TopicPartition(topic_name, 1), TopicPartition(topic_name, 2),
                              TopicPartition(topic_name, 3)])
        self.files = {}
        self.csv_files = {}
        self.header = ['author_id', 'created_at', 'text']

    def read_tweets(self):
        while True:
            msg = next(self.consumer)
            value = msg.value.decode('utf-8')
            json_tweet = json.loads(value)
            author_id = json_tweet["author_id"]
            created_at = json_tweet["created_at"]
            text = json_tweet["text"]
            time_struct = time.strptime(created_at, "%a, %b %d %H:%M:%S %z %Y")
            file_name = "tweets_%02d_%02d_%d_%02d_%02d.csv" % (time_struct.tm_mday, time_struct.tm_mon,
                                                               time_struct.tm_year, time_struct.tm_hour,
                                                               time_struct.tm_min)
            if file_name not in self.files.keys():
                self.files[file_name] = open("/opt/app/tweets/" + file_name, "w+", newline='')
                self.csv_files[file_name] = csv.writer(self.files[file_name], delimiter=",", quoting=csv.QUOTE_MINIMAL)
                self.csv_files[file_name].writerow(self.header)
                self.csv_files[file_name].writerow([author_id, created_at, text])

    def __del__(self):
        for key in self.files.keys():
            self.files[key].close()


def main():
    kafka_host_name = "kafka-server:9092"
    topic_name = "tweets"
    kafka_tweets = KafkaTweetsReader(kafka_host_name, topic_name)
    kafka_tweets.read_tweets()


if __name__ == "__main__":
    main()
