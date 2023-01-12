
import argparse
from datetime import datetime
from threading import Thread

from transaction_producer import get_message
from kafka_client import MyKafkaClient

TRANSACTIONS_NUM_DEFAULT = 25000
TRANSACTIONS_MEMORY_NUM_DEFAULT = 1000

def create_transactions(num, memory_msgs):
    for _ in range(num):
        transaction = get_message(
            existing_account_prob = 0.5,
            existing_recipient_prob = 0.5,
            max_devices_per_account = 5,
            new_device_prob = 0.1,
            max_locations_per_account = 10,
            new_location_prob = 0.5,
            keep_transaction_in_memory = True,
            transactions_in_memory_num = memory_msgs
        )

def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument('--kafka_host', type=str, required=True)
    parser.add_argument('--kafka_port', type=int, required=True)
    parser.add_argument('--kafka_topic', type=str, required=True)
    parser.add_argument(
        '--msg_s', 
        nargs='?', 
        type=int, 
        const=TRANSACTIONS_NUM_DEFAULT, 
        default=TRANSACTIONS_NUM_DEFAULT
    )
    parser.add_argument(
        '--memory_msgs', 
        nargs='?', 
        type=int, 
        const=TRANSACTIONS_MEMORY_NUM_DEFAULT, 
        default=TRANSACTIONS_MEMORY_NUM_DEFAULT
    )

    return parser.parse_args()

if __name__== "__main__" :
    args = parse_arguments()
    
    kafka_client = MyKafkaClient(args.kafka_host, args.kafka_port, args.kafka_topic, 1)

    num_threads = 4
    transactions_per_thread = int(args.msg_s / num_threads)
    threads = []

    while True:
        start_time = datetime.now()

        for i in range(num_threads):
            thread = Thread(target=create_transactions, args=(transactions_per_thread, args.memory_msgs))
            threads.append(thread)
            thread.start()
        
        for index, thread in enumerate(threads):
            thread.join()

        # for _ in range(args.msg_s):
        #     transaction = get_message(
        #         existing_account_prob = 0.5,
        #         existing_recipient_prob = 0.5,
        #         max_devices_per_account = 5,
        #         new_device_prob = 0.1,
        #         max_locations_per_account = 10,
        #         new_location_prob = 0.5,
        #         keep_transaction_in_memory = True,
        #         transactions_in_memory_num = args.memory_msgs
        #     )

        end_time = datetime.now()
        diff = end_time - start_time
        msg = "Number of messages: {}; Duration in seconds: {}".format(args.msg_s, diff.total_seconds())

        print(msg)
        # kafka_client.send_transaction(msg)
