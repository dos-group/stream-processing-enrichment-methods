import argparse
import time
from datetime import datetime, timedelta
import threading
import uuid

from transaction_producer import get_message
from transaction_producer import get_account_info
from transaction_producer import add_account_info
from transaction_producer import update_recipients
from transaction_producer import get_timestamp

from log_producer import LogProducer

from cassandra_client import CassandraTransactionClient
from kafka_client import MyKafkaClient

BATCH_SIZE_DB_DEFAULT = 10
DB_RETRIEVE_LIMIT_DEFAULT = 10

KAFKA_START_TIME_DEFAULT = str(int(time.time() + 10))
KAFKA_START_TIME_OFFSET_DEFAULT = 10
KAFKA_TRANSACTIONS_PER_SEC_START = 2
KAFKA_TRANSACTIONS_PER_SEC_END = 10
KAFKA_TRANSACTIONS_PER_SEC_STEPS = 2
KAFKA_INCREASE_MSG_S = "False"
KAFKA_ITER_PER_SIZE = 60 * 5

TRANSACTION_EVENT_TYPE = "Transaction"
Log_EVENT_TYPE = "Log"
EVENT_TYPE_DEFAULT = TRANSACTION_EVENT_TYPE

EXISTING_ACCOUNT_PROB_DEFAULT = 0
EXISTING_RECIPIENT_PROB_DEFAULT = 0

MAX_DEVICES_PER_ACCOUNT_DEFAULT = 1
NEW_DEVICE_PROB_DEFAULT = 0

MAX_LOCATIONS_PER_ACCOUNT_DEFAULT = 1
NEW_LOCATION_PROB_DEFAULT = 0


def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--enrichment_type',
        nargs='?',
        type=str,
        const='nd',
        default='nd'
    )

    parser.add_argument(
        '--event_type',
        nargs='?',
        type=str,
        choices=[TRANSACTION_EVENT_TYPE, Log_EVENT_TYPE],
        const=EVENT_TYPE_DEFAULT,
        default=EVENT_TYPE_DEFAULT
    )

    parser.add_argument('--db_host', type=str, required=True)
    parser.add_argument('--db_port', type=int, required=True)
    parser.add_argument('--db_keyspace', type=str, required=True)
    parser.add_argument('--db_user', type=str, required=True)
    parser.add_argument('--db_password', type=str, required=True)
    parser.add_argument(
        '--db_retrieve_limit',
        nargs='?',
        type=int,
        const=DB_RETRIEVE_LIMIT_DEFAULT,
        default=DB_RETRIEVE_LIMIT_DEFAULT
    )

    parser.add_argument('--kafka_host', type=str, required=True)
    parser.add_argument('--kafka_port', type=int, required=True)
    parser.add_argument('--kafka_topic', type=str, required=True)
    parser.add_argument(
        '--kafka_increase_msg_s',
        nargs='?',
        type=str,
        const=KAFKA_INCREASE_MSG_S,
        default=KAFKA_INCREASE_MSG_S
    )
    parser.add_argument(
        '--kafka_start_time',
        nargs='?',
        type=str,
        const=KAFKA_START_TIME_DEFAULT,
        default=KAFKA_START_TIME_DEFAULT
    )
    parser.add_argument(
        '--kafka_iter_per_size',
        nargs='?',
        type=int,
        const=KAFKA_ITER_PER_SIZE,
        default=KAFKA_ITER_PER_SIZE
    )
    parser.add_argument(
        '--kafka_msg_s_start',
        nargs='?',
        type=int,
        const=KAFKA_TRANSACTIONS_PER_SEC_START,
        default=KAFKA_TRANSACTIONS_PER_SEC_START
    )
    parser.add_argument(
        '--kafka_msg_s_end',
        nargs='?',
        type=int,
        const=KAFKA_TRANSACTIONS_PER_SEC_END,
        default=KAFKA_TRANSACTIONS_PER_SEC_END
    )
    parser.add_argument(
        '--kafka_msg_s_steps',
        nargs='?',
        type=int,
        const=KAFKA_TRANSACTIONS_PER_SEC_STEPS,
        default=KAFKA_TRANSACTIONS_PER_SEC_STEPS
    )

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


db_throughput_update = {
    "hostname": None,
    "throughput_start": None,
    "throughput_end": None,
    "throughput": None,
    "enrichment_type": None
}


def update_db_thread_increasing_msgs(args, event):
    print("UPDATE DB THREAD INCREASING START!")

    client = CassandraTransactionClient(
        host=args.db_host,
        port=args.db_port,
        keyspace=args.db_keyspace,
        user=args.db_user,
        password=args.db_password
    )

    start = args.kafka_msg_s_start
    end = args.kafka_msg_s_end
    steps = args.kafka_msg_s_steps

    for msg_s in range(start, end + steps, steps):
        event.wait()
        client.insert_metrics(
            db_throughput_update["hostname"],
            db_throughput_update["throughput_start"],
            db_throughput_update["throughput_end"],
            db_throughput_update["throughput"],
            db_throughput_update["enrichment_type"]
        )
        print(db_throughput_update)
        event.clear()

    print("UPDATE DB THREAD INCREASING END!")


def update_db_thread_static_msgs(args, event):
    print("UPDATE DB THREAD STATIC START!")

    client = CassandraTransactionClient(
        host=args.db_host,
        port=args.db_port,
        keyspace=args.db_keyspace,
        user=args.db_user,
        password=args.db_password
    )

    event.wait()
    client.insert_metrics(
        db_throughput_update["hostname"],
        db_throughput_update["throughput_start"],
        db_throughput_update["throughput_end"],
        db_throughput_update["throughput"],
        db_throughput_update["enrichment_type"]
    )
    print(db_throughput_update)
    event.clear()

    print("UPDATE DB THREAD STATIC END!")


def write_increasing_transactions_to_kafka(args):
    start_time = datetime.utcfromtimestamp(int(args.kafka_start_time))

    start = args.kafka_msg_s_start
    end = args.kafka_msg_s_end
    steps = args.kafka_msg_s_steps

    # use transaction retrieve limit as max keys for log events
    log_producer = LogProducer(args.db_retrieve_limit)

    # return if time has passed all iterations
    num_iterations = len(list(range(start, end + steps, steps)))
    if datetime.utcnow() > start_time + timedelta(seconds=args.kafka_iter_per_size * num_iterations):
        return

    ev1 = threading.Event()
    th1 = threading.Thread(target=update_db_thread_increasing_msgs, args=(args, ev1))
    th1.start()

    clients = {}
    for msg_s in range(start, end + steps, steps):
        client = MyKafkaClient(
            host=args.kafka_host,
            port=args.kafka_port,
            topic=args.kafka_topic,
            msg_s=msg_s
        )
        clients[msg_s] = client

    while datetime.utcnow() < start_time:
        time.sleep(0.001)

    iteration_time = timedelta(seconds=args.kafka_iter_per_size)
    next_iteration_start_time = start_time + iteration_time

    db_throughput_update["hostname"] = str(uuid.uuid4())
    db_throughput_update["enrichment_type"] = args.enrichment_type

    for msg_s in range(start, end + steps, steps):
        db_throughput_update["throughput_start"] = next_iteration_start_time - iteration_time
        db_throughput_update["throughput_end"] = next_iteration_start_time
        db_throughput_update["throughput"] = msg_s
        ev1.set()

        client = clients[msg_s]

        while datetime.utcnow() < next_iteration_start_time:
            if args.event_type == TRANSACTION_EVENT_TYPE:
                send_transaction_batch(args, client, msg_s)
            elif args.event_type == Log_EVENT_TYPE:
                send_log_batch(log_producer, client, msg_s)

        next_iteration_start_time = next_iteration_start_time + iteration_time


def write_static_transactions_to_kafka(args):
    client = MyKafkaClient(
        host=args.kafka_host,
        port=args.kafka_port,
        topic=args.kafka_topic,
        msg_s=args.kafka_msg_s_start
    )

    start_time = datetime.utcfromtimestamp(int(args.kafka_start_time))

    msg_s = args.kafka_msg_s_start

    # use transaction retrieve limit as max keys for log events
    log_producer = LogProducer(args.db_retrieve_limit)

    # return if time has passed the iteration
    if datetime.utcnow() > start_time + timedelta(seconds=args.kafka_iter_per_size):
        return

    # ev1 = threading.Event()
    # th1 = threading.Thread(target=update_db_thread_static_msgs, args=(args, ev1))
    # th1.start()

    iteration_time = timedelta(seconds=args.kafka_iter_per_size)
    stop_time = start_time + iteration_time

    db_throughput_update["hostname"] = str(uuid.uuid4())
    db_throughput_update["enrichment_type"] = args.enrichment_type
    db_throughput_update["throughput_start"] = start_time
    db_throughput_update["throughput_end"] = stop_time
    db_throughput_update["throughput"] = msg_s

    while datetime.utcnow() < start_time:
        time.sleep(0.001)

    # ev1.set()

    while datetime.utcnow() < stop_time:
        if args.event_type == TRANSACTION_EVENT_TYPE:
            send_transaction_batch(args, client, msg_s)
        elif args.event_type == Log_EVENT_TYPE:
            send_log_batch(log_producer, client, msg_s)


def send_log_batch(log_producer, kafka_client, num):
    start_collection_transaction_time = datetime.utcnow()

    for _ in range(num):
        log_event = log_producer.get_message()
        kafka_client.send_msg(log_event)

    end_collection_transaction_time = datetime.utcnow()
    diff = end_collection_transaction_time - start_collection_transaction_time
    if diff.total_seconds() < 1:
        time.sleep(1 - diff.total_seconds())


def send_transaction_batch(args, kafka_client, num):
    start_collection_transaction_time = datetime.utcnow()

    transactions = []
    for _ in range(num):
        transaction = get_message(
            existing_account_prob=args.existing_account_prob,
            existing_recipient_prob=args.existing_recipient_prob,
            max_devices_per_account=args.max_devices_per_account,
            new_device_prob=args.new_device_prob,
            max_locations_per_account=args.max_locations_per_account,
            new_location_prob=args.new_location_prob
        )
        transactions.append(transaction)

    ts = get_timestamp()
    for transaction in transactions:
        transaction["transaction"]["timestamp"] = ts
        kafka_client.send_msg(transaction)

    end_collection_transaction_time = datetime.utcnow()
    diff = end_collection_transaction_time - start_collection_transaction_time
    if diff.total_seconds() < 1:
        time.sleep(1 - diff.total_seconds())


def write_transaction_to_kafka(args):
    if str_to_boolean(args.kafka_increase_msg_s):
        write_increasing_transactions_to_kafka(args)
    else:
        write_static_transactions_to_kafka(args)


def str_to_boolean(value):
    value_upper = str(value).upper()
    if 'TRUE'.startswith(value_upper):
        return True
    elif 'FALSE'.startswith(value_upper):
        return False
    else:
        return False


def add_transactions_to_memory(args):
    client = CassandraTransactionClient(
        host=args.db_host,
        port=args.db_port,
        keyspace=args.db_keyspace,
        user=args.db_user,
        password=args.db_password
    )
    transaction_rows = client.get_transactions(args.db_retrieve_limit)
    account_ids = []
    for transaction_row in transaction_rows:
        if transaction_row.account_id in account_ids:
            update_recipients(transaction_row.account_id, transaction_row.recipient_id)
            continue

        device_rows = client.get_devices(transaction_row.account_id)
        location_rows = client.get_locations(transaction_row.account_id)

        account_info = get_account_info(transaction_row, device_rows, location_rows)
        add_account_info(transaction_row.account_id, account_info, args.db_retrieve_limit)

        account_ids.append(transaction_row.account_id)


if __name__ == "__main__":
    args = parse_arguments()

    if args.event_type == TRANSACTION_EVENT_TYPE:
        add_transactions_to_memory(args)
    write_transaction_to_kafka(args)
    print("main.py ran successfully")

    """
    (1) retrieve transactions from db

    (2) generate transactions and push them to kafka
            - use transactions events from db to make 
              use of exsting account_ids, recipient_ids etc.
            
            - two producer options
                (i) increase throughput over time
                (ii) static throughput

    """
