# This code fetches Ethereum trades in real-time from Bitquery Kafka Streams in Protobuf format
import os
import uuid
from confluent_kafka import Consumer, KafkaError, KafkaException
from google.protobuf.message import DecodeError
from evm import dex_block_message_pb2
import binascii
import base58
import json
from helpers.convert_bytes import convert_bytes 
from helpers.print_protobuf_message import print_protobuf_message
group_id_suffix = uuid.uuid4().hex

conf = {
    'bootstrap.servers': 'rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092',
    'group.id': f'bsc_104-group-{group_id_suffix}',  
    'session.timeout.ms': 30000,
    'security.protocol': 'SASL_PLAINTEXT',
    'ssl.endpoint.identification.algorithm': 'none',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': 'bsc_104',
    'sasl.password': 'WpMNDhyMXJUKOz20rppMlcvD4HzJqb',
    'auto.offset.reset': 'latest',
}

consumer = Consumer(conf)
topic = 'bsc.dextrades.proto'
consumer.subscribe([topic])

def process_message(message):
    try:
        buffer = message.value()
        parsed_message = dex_block_message_pb2.DexBlockMessage()
        parsed_message.ParseFromString(buffer)
        trades = parsed_message.Trades

        for trade in trades:
            dex = trade.Dex
            address = dex.SmartContract
            address = convert_bytes(address)

            if address == "5c952063c7fc8610ffdb798152d69f0b9550762b":
                print("\nNew DexBlockMessage received:\n")
                print_protobuf_message(parsed_message, encoding='base58')
            else:
                print("Trade in another DEX")
    except DecodeError as err:
        print(f"Protobuf decoding error: {err}")
    except Exception as err:
        print(f"Error processing message: {err}")



try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())
        process_message(msg)

except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()
