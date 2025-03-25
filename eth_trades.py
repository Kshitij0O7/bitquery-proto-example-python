# This code fetches Ethereum trades in real-time from Bitquery Kafka Streams in Protobuf format
import os
import uuid
from confluent_kafka import Consumer, KafkaError, KafkaException
from google.protobuf.message import DecodeError
from google.protobuf.descriptor import FieldDescriptor
from evm import dex_block_message_pb2
import binascii
import base58
import json
group_id_suffix = uuid.uuid4().hex

conf = {
    'bootstrap.servers': 'rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092',
    'group.id': f'eth_102-group-{group_id_suffix}',  
    'session.timeout.ms': 30000,
    'security.protocol': 'SASL_PLAINTEXT',
    'ssl.endpoint.identification.algorithm': 'none',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': 'usernameeee',
    'sasl.password': 'pwwww',
    'auto.offset.reset': 'latest',
}

consumer = Consumer(conf)
topic = 'eth.dextrades.proto'
consumer.subscribe([topic])


def convert_bytes(value, encoding='hex'):
    if encoding == 'base58':
        return base58.b58encode(value).decode()
    else:
        return value.hex()

def print_protobuf_message(msg, indent=0, encoding='base58'):
    prefix = ' ' * indent
    for field in msg.DESCRIPTOR.fields:
        value = getattr(msg, field.name)

        if field.label == FieldDescriptor.LABEL_REPEATED: # list or array
            if not value:
                continue
            print(f"{prefix}{field.name} (repeated):")
            for idx, item in enumerate(value):
                if field.type == FieldDescriptor.TYPE_MESSAGE:
                    print(f"{prefix}  [{idx}]:")
                    print_protobuf_message(item, indent + 4, encoding)
                elif field.type == FieldDescriptor.TYPE_BYTES:
                    print(f"{prefix}  [{idx}]: {convert_bytes(item, encoding)}")
                else:
                    print(f"{prefix}  [{idx}]: {item}")

        elif field.type == FieldDescriptor.TYPE_MESSAGE: # nested protobuf message
            if msg.HasField(field.name):
                print(f"{prefix}{field.name}:")
                print_protobuf_message(value, indent + 4, encoding)

        elif field.type == FieldDescriptor.TYPE_BYTES:  # need to be converted
            print(f"{prefix}{field.name}: {convert_bytes(value, encoding)}")

        elif field.containing_oneof:
            # Only print the active oneof field
            if msg.WhichOneof(field.containing_oneof.name) == field.name:
                print(f"{prefix}{field.name} (oneof): {value}")

        else:
            print(f"{prefix}{field.name}: {value}")




def process_message(message):
    try:
        buffer = message.value()
        parsed_message = dex_block_message_pb2.DexBlockMessage()
        parsed_message.ParseFromString(buffer)

        print("\nNew DexBlockMessage received:\n")

        print_protobuf_message(parsed_message, encoding='base58')


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
