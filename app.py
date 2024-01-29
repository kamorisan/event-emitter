import argparse
import csv
import json
import logging
import os
import random
import time
import uuid

from kafka import KafkaProducer

# Function to load data from CSV file
def load_csv_data(file_path):
    data = []
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            data.append(row[0])  # Assuming the item names are in the first column
    return data

ORDER_TYPES = ["E", "F", "G"]
#ORDER_ITEM_NAMES = ["Lime", "Lemon Bar", "Fruit Punch"]
ORDER_ITEM_NAMES = load_csv_data("item_names.csv")
#QUANTITIES
#PRICES
SHIPMENT_ADDRESSES = ["541-428 Nulla Avenue", "Ap #249-5876 Magna. Rd.", "525-8975 Urna. Street"]
ZIP_CODES = ["4286", "I9E 0JN", "13965"]
#クレジッドカード情報
## CC_NUMBER
## CC_EXPIRY_DATE
## CC_TYPE
## NAME



def generate_event():
    ret = {
          "orderType": random.choice(ORDER_TYPES),
          "orderItemName": random.choice(ORDER_ITEM_NAMES),
          "quantity": random.randint(10, 200),
          "price": round(random.uniform(0.01, 9.99), 2),
          "shipmentAddress": random.choice(SHIPMENT_ADDRESSES),
          "zipCode": random.choice(ZIP_CODES)
        }
    return ret

def main(args):
    logging.info('brokers={}'.format(args.brokers))
    logging.info('topic={}'.format(args.topic))
    logging.info('rate={}'.format(args.rate))

    logging.info('creating kafka producer')
    producer = KafkaProducer(bootstrap_servers=args.brokers)

    logging.info('begin sending events')
    while True:

        producer.send(args.topic,json.dumps(generate_event()).encode() , 'cust567'.encode())
        time.sleep(float(args.rate))
    logging.info('end sending events')


def get_arg(env, default):
    return os.getenv(env) if os.getenv(env, '') is not '' else default


def parse_args(parser):
    args = parser.parse_args()
    args.brokers = get_arg('KAFKA_BROKERS', args.brokers)
    args.topic = get_arg('KAFKA_TOPIC', args.topic)
    args.rate = get_arg('RATE', args.rate)
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info('starting kafka-openshift-python emitter')
    parser = argparse.ArgumentParser(description='emit some stuff on kafka')
    parser.add_argument(
        '--brokers',
        help='The bootstrap servers, env variable KAFKA_BROKERS',
        default='localhost:9092')
    parser.add_argument(
        '--topic',
        help='Topic to publish to, env variable KAFKA_TOPIC',
        default='event-input-stream')
    parser.add_argument(
        '--rate',
        type=int,
        help='Lines per second, env variable RATE',
        default=1)
    args = parse_args(parser)
    main(args)
    logging.info('exiting')
