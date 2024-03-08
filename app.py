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
FIRST_NAME = load_csv_data("csvdata/first_name.csv")
LAST_NAME = load_csv_data("csvdata/last_name.csv")
ORDER_ITEM_NAMES = load_csv_data("csvdata/order_item_names.csv")
SHIPMENT_ADDRESSES = load_csv_data("csvdata/shipment_addresses.csv")
CC_TYPE = load_csv_data("csvdata/cc_type.csv")

def generate_event():
    ret = {
        "orderID": str(uuid.uuid4()),  # UUIDを文字列に変換する
        "orderItemName": random.choice(ORDER_ITEM_NAMES),
        "firstName": random.choice(FIRST_NAME),
        "lastName": random.choice(LAST_NAME),
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
    logging.info('iterations={}'.format(args.iterations))

    logging.info('creating kafka producer')
    producer = KafkaProducer(bootstrap_servers=args.brokers)

    count = 0  # 送信回数を追跡するカウンター
    logging.info('begin sending events')
    while True:
        if args.iterations != -1 and count >= args.iterations:
            break  # 指定された回数に達したらループを終了
        
        event = generate_event()  # イベントを生成
        event_key = event["orderID"].encode()  # orderIDをキーとしてエンコード
        event_value = json.dumps(event).encode()  # イベントデータをJSON形式に変換し、バイト列にエンコード

        producer.send(args.topic, key=event_key, value=event_value)  # キーと値を指定して送信
        time.sleep(float(args.rate))
        count += 1  # カウンターをインクリメント

    logging.info('end sending events')


def get_arg(env, default):
    return os.getenv(env) if os.getenv(env, '') is not '' else default


def parse_args(parser):
    args = parser.parse_args()
    args.brokers = get_arg('KAFKA_BROKERS', args.brokers)
    args.topic = get_arg('KAFKA_TOPIC', args.topic)
    args.rate = get_arg('RATE', args.rate)
    args.iterations = int(get_arg('ITERATIONS', args.iterations))  # 環境変数からの読み込みをサポート
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
    parser.add_argument(
        '--iterations',
        type=int,
        help='Number of events to send, env variable ITERATIONS. -1 for infinite loop',
        default=-1)
    args = parse_args(parser)
    main(args)
    logging.info('exiting')
