# This code filters trades of a token on Solana 

import json
import os
import uuid
import base58
from confluent_kafka import Consumer, KafkaError, KafkaException
from google.protobuf.message import DecodeError
from solana import dex_block_message_pb2  # Import correct protobuf file

# Unique consumer group ID
group_id_suffix = uuid.uuid4().hex
conf = {
    'bootstrap.servers': 'rpk0.bitquery.io:9093,rpk1.bitquery.io:9093,rpk2.bitquery.io:9093',
    'group.id': f'usernameee-group-{group_id_suffix}',
    'session.timeout.ms': 30000,
    'security.protocol': 'SASL_SSL',
    'ssl.ca.location': '/path-to/server.cer.pem',
    'ssl.key.location': '/path-to//client.key.pem',
    'ssl.certificate.location': '/path-to//client.cer.pem',
    'ssl.endpoint.identification.algorithm': 'none',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': 'usernameee',
    'sasl.password': 'pwww',
    'auto.offset.reset': 'latest',
}

consumer = Consumer(conf)
topic = 'solana.dextrades.proto'
consumer.subscribe([topic])

# The BaseCurrency we are checking for (Base58 format)
TARGET_BASE_CURRENCY = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
SOLANA_SYSTEM_PROGRAM = "So11111111111111111111111111111111111111112"  # Ignore this

def process_message(message):
    try:
        buffer = message.value()
        
        # Parse Protobuf message
        parsed_message = dex_block_message_pb2.DexParsedBlockMessage()
        parsed_message.ParseFromString(buffer)

        for txn in parsed_message.Transactions:
            for trade in txn.Trades:
                # Decode BaseCurrency
                base_currency_address = base58.b58encode(trade.Market.BaseCurrency.MintAddress).decode()

                # Check if BaseCurrency matches the target
                if base_currency_address == TARGET_BASE_CURRENCY:
                    print(f" Matching Transaction Found!")
                    print(f"Transaction Signature: {base58.b58encode(txn.Signature).decode()}")
                    print(f"BaseCurrency: {base_currency_address}")
                    print(f"Trade Amount: {trade.Buy.Amount}")

    except DecodeError as err:
        print(f" Protobuf decoding error: {err}")
    except Exception as err:
        print(f" Error processing message: {err}")

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
    pass
finally:
    consumer.close()
