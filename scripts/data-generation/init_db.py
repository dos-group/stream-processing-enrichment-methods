import argparse
import time
from tqdm import tqdm

from transaction_producer import get_message, get_timestamp
from cassandra_client import CassandraTransactionClient
from kafka_client import MyKafkaClient

BATCH_SIZE_DB_DEFAULT = 10
NUM_TRANSACTIONS_DB_DEFAULT = 100
NUM_TRANSACTIONS_MEMORY_DEFAULT = 100

WRITE_TO_KAFKA = False

EXISTING_ACCOUNT_PROB_DEFAULT = 1
EXISTING_RECIPIENT_PROB_DEFAULT = 0

MAX_DEVICES_PER_ACCOUNT_DEFAULT = 1
NEW_DEVICE_PROB_DEFAULT = 0

MAX_LOCATIONS_PER_ACCOUNT_DEFAULT = 1
NEW_LOCATION_PROB_DEFAULT = 0


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db_host', type=str, required=True)
    parser.add_argument('--db_port', type=int, required=True)
    parser.add_argument('--db_keyspace', type=str, required=True)
    parser.add_argument('--db_user', type=str, required=True)
    parser.add_argument('--db_password', type=str, required=True)
    parser.add_argument(
        '--db_msg_num',
        nargs='?',
        type=int,
        const=NUM_TRANSACTIONS_DB_DEFAULT,
        default=NUM_TRANSACTIONS_DB_DEFAULT
    )
    parser.add_argument(
        '--batch_size',
        nargs='?',
        type=int,
        const=BATCH_SIZE_DB_DEFAULT,
        default=BATCH_SIZE_DB_DEFAULT
    )

    parser.add_argument(
        '--write_to_kafka',
        nargs='?',
        type=str,
        const=WRITE_TO_KAFKA,
        default=WRITE_TO_KAFKA
    )
    parser.add_argument('--kafka_host', type=str)
    parser.add_argument('--kafka_port', type=int)
    parser.add_argument('--kafka_topic', type=str)

    parser.add_argument(
        '--existing_account_prob',
        nargs='?',
        type=float,
        const=EXISTING_ACCOUNT_PROB_DEFAULT,
        default=EXISTING_ACCOUNT_PROB_DEFAULT
    )
    parser.add_argument(
        '--existing_recipient_prob',
        nargs='?',
        type=float,
        const=EXISTING_RECIPIENT_PROB_DEFAULT,
        default=EXISTING_RECIPIENT_PROB_DEFAULT
    )
    parser.add_argument(
        '--max_devices_per_account',
        nargs='?',
        type=float,
        const=MAX_DEVICES_PER_ACCOUNT_DEFAULT,
        default=MAX_DEVICES_PER_ACCOUNT_DEFAULT
    )
    parser.add_argument(
        '--new_device_prob',
        nargs='?',
        type=float,
        const=NEW_DEVICE_PROB_DEFAULT,
        default=NEW_DEVICE_PROB_DEFAULT
    )
    parser.add_argument(
        '--max_locations_per_account',
        nargs='?',
        type=float,
        const=MAX_LOCATIONS_PER_ACCOUNT_DEFAULT,
        default=MAX_LOCATIONS_PER_ACCOUNT_DEFAULT
    )
    parser.add_argument(
        '--new_location_prob',
        nargs='?',
        type=float,
        const=NEW_LOCATION_PROB_DEFAULT,
        default=NEW_LOCATION_PROB_DEFAULT
    )

    return parser.parse_args()


def get_bool(value):
    value_upper = str(value).upper()
    if 'TRUE'.startswith(value_upper):
        return True
    elif 'FALSE'.startswith(value_upper):
        return False
    else:
        return False  # temporary


def write_transaction_to_db(args):
    cassandra_client = CassandraTransactionClient(
        host=args.db_host,
        port=args.db_port,
        keyspace=args.db_keyspace,
        user=args.db_user,
        password=args.db_password
    )

    write_to_kafka = get_bool(args.write_to_kafka)

    kafka_client = None
    if write_to_kafka:
        kafka_client = MyKafkaClient(
            host=args.kafka_host,
            port=args.kafka_port,
            topic=args.kafka_topic,
            msg_s=10
        )

    transactions = []
    for i in tqdm(range(args.db_msg_num)):
        transaction = get_message(
            existing_account_prob=args.existing_account_prob,
            existing_recipient_prob=args.existing_recipient_prob,
            max_devices_per_account=args.max_devices_per_account,
            new_device_prob=args.new_device_prob,
            max_locations_per_account=args.max_locations_per_account,
            new_location_prob=args.new_location_prob,
            keep_transaction_in_memory=True,
            transactions_in_memory_num=NUM_TRANSACTIONS_MEMORY_DEFAULT
        )

        if write_to_kafka:
            kafka_client.send_msg(transaction)

        transaction["transaction"]["timestamp"] = get_timestamp()

        transactions.append(transaction)

        cassandra_client.insert_device(transaction)
        cassandra_client.insert_location(transaction)

        if len(transactions) >= args.batch_size:
            cassandra_client.insert_transaction_batch(transactions)
            transactions.clear()


if __name__ == "__main__":
    args = parse_arguments()

    write_transaction_to_db(args)
    print("init db successfully")
