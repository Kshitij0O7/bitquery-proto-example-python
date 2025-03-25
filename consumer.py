# This code displays latest transactions on Solana 

import json
import uuid
import base58
from confluent_kafka import Consumer, KafkaError, KafkaException
from google.protobuf.message import DecodeError
from google.protobuf.descriptor import FieldDescriptor
from solana import parsed_idl_block_message_pb2

# Kafka consumer configuration
group_id_suffix = uuid.uuid4().hex
conf = {
    'bootstrap.servers': 'rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092',
    'group.id': f'username-group-{group_id_suffix}',  
    'session.timeout.ms': 30000,
    'security.protocol': 'SASL_PLAINTEXT',
    'ssl.endpoint.identification.algorithm': 'none',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': 'usernamee',
    'sasl.password': 'pwww',
    'auto.offset.reset': 'latest',
}

consumer = Consumer(conf)
topic = 'solana.transactions.proto'
consumer.subscribe([topic])

# --- Helpers for traversal and decoding --- #

def convert_bytes(value, encoding='base58'):
    """Convert raw protobuf bytes into base58 or hex."""
    if encoding == 'base58':
        return base58.b58encode(value).decode()
    return value.hex()

def to_serializable_dict(msg, encoding='base58'):
    """Recursively convert protobuf message into a JSON-serializable dictionary."""
    result = {}
    for field in msg.DESCRIPTOR.fields:
        value = getattr(msg, field.name)

        if field.label == FieldDescriptor.LABEL_REPEATED:
            if field.type == FieldDescriptor.TYPE_MESSAGE:
                result[field.name] = [to_serializable_dict(v, encoding) for v in value]
            elif field.type == FieldDescriptor.TYPE_BYTES:
                result[field.name] = [convert_bytes(v, encoding) for v in value]
            else:
                result[field.name] = list(value)

        elif field.type == FieldDescriptor.TYPE_MESSAGE:
            if msg.HasField(field.name):
                result[field.name] = to_serializable_dict(value, encoding)

        elif field.type == FieldDescriptor.TYPE_BYTES:
            result[field.name] = convert_bytes(value, encoding)

        elif field.containing_oneof:
            if msg.WhichOneof(field.containing_oneof.name) == field.name:
                result[field.name] = value

        else:
            result[field.name] = value

    return result

# --- Message processing --- #

def process_message(message):
    try:
        buffer = message.value()

        # Parse protobuf message
        tx_block = parsed_idl_block_message_pb2.ParsedIdlBlockMessage()
        tx_block.ParseFromString(buffer)

        print("\nNew ParsedIdlBlockMessage received:\n")

        # Convert to dict with base58 encoding
        message_dict = to_serializable_dict(tx_block, encoding='base58')

        # Pretty print JSON
        print(json.dumps(message_dict, indent=2))

    except DecodeError as err:
        print(f"Protobuf decoding error: {err}")
    except Exception as err:
        print(f"Error processing message: {err}")

# --- Start polling --- #

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
    print("Stopping consumer...")
finally:
    consumer.close()
