import argparse
import json
import logging
import os
import random
import time
import uuid

from kafka import KafkaProducer

EVENT_TEMPLATES = [
  {
    "orderType": "E",
    "orderItemName": "Lime",
    "quantity": 100,
    "price": 3.69,
    "shipmentAddress": "541-428 Nulla Avenue",
    "zipCode": "4286"
  },
  {
    "orderType": "E",
    "orderItemName": "Lemon Bar",
    "quantity": 17,
    "price": 0.09,
    "shipmentAddress": "Ap #249-5876 Magna. Rd.",
    "zipCode": "I9E 0JN"
  },
  {
    "orderType": "E",
    "orderItemName": "Fruit Punch",
    "quantity": 16,
    "price": 7.76,
    "shipmentAddress": "525-8975 Urna. Street",
    "zipCode": "13965"
  }
]

def generate_event():
    ret = EVENT_TEMPLATES[random.randrange(1000)]
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
