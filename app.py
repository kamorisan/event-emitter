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
            # Clean each item in the row (removing extra spaces and quotes)
            cleaned_row = [item.strip().strip('"') for item in row]
            # Remove empty strings from the cleaned row
            cleaned_row = [item for item in cleaned_row if item]  # Filter out empty strings
            data.extend(cleaned_row)
    return data

# DATA LOAD
ORDER_ITEM_NAMES = load_csv_data("csvdata/order_item_names.csv")
SHIPMENT_ADDRESSES = load_csv_data("csvdata/shipment_addresses.csv")
#クレジッドカード情報
## CC_NUMBER
## CC_EXPIRY_DATE
CC_TYPE = load_csv_data("csvdata/cc_type.csv")
## NAME

# ランダムな4桁の数を4組生成し、ハイフンで連結する
random_numbers = [str(random.randint(0, 9999)).zfill(4) for _ in range(4)]
CC_NUMBER = "-".join(random_numbers)

def generate_event():
    ret = {
        "orderID": str(uuid.uuid4()),  # UUIDを文字列に変換する
        "orderItemName": random.choice(ORDER_ITEM_NAMES),
        "quantity": random.randint(10, 200),
        "price": round(random.uniform(0.01, 9.99), 2),
        "shipmentAddress": random.choice(SHIPMENT_ADDRESSES),
        "zipCode": random.randint(10000, 99999),
        "creditCardNumber": "-".join([str(random.randint(0, 9999)).zfill(4) for _ in range(4)]),
        "creditCardExpiryDate": "{:04d}/{:02d}".format(random.randint(2023, 2030), random.randint(1, 12)),
        "creditCardType": random.choice(CC_TYPE)
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
