import configparser
import os
import time
from faker import Faker
from confluent_kafka import Producer
import json

script_dir = os.path.dirname(__file__)
config = configparser.ConfigParser()
config_file_path = os.path.join(script_dir, 'config/config.ini')
config.read(config_file_path)

TOPIC = config['KAFKA']['TOPIC']
BOOTSTRAP_SERVER = config['KAFKA']['BOOTSTRAP_SERVER']


def send_to_kafka(message):
    p = Producer({'bootstrap.servers': BOOTSTRAP_SERVER})
    p.produce(TOPIC, json.dumps(message))
    p.flush()


def generate_random_message():
    faker = Faker()
    message = {
        "id": faker.uuid4(),
        "name": faker.name(),
        "address": faker.address(),
        "email": faker.email(),
        "job": faker.job(),
        "company": faker.company(),
        "phone_number": faker.phone_number()
    }
    return message


if __name__ == '__main__':
    while True:
        message = generate_random_message()
        send_to_kafka(message)
        print("Message sent to Kafka")
        time.sleep(0.3)

