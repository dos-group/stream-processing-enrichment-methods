import argparse

from transaction_producer import get_message_by_db_rows
from cassandra_client import CassandraTransactionClient
from kafka_client import MyKafkaClient


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db_host', type=str, required=True)
    parser.add_argument('--db_port', type=int, required=True)
    parser.add_argument('--db_keyspace', type=str, required=True)
    parser.add_argument('--db_user', type=str, required=True)
    parser.add_argument('--db_password', type=str, required=True)

    parser.add_argument('--kafka_host', type=str, required=True)
    parser.add_argument('--kafka_port', type=int, required=True)
    parser.add_argument('--kafka_topic', type=str, required=True)

    return parser.parse_args()


def send_db_rows_to_kafka(args):
    cassandra_client = CassandraTransactionClient(
        host=args.db_host,
        port=args.db_port,
        keyspace=args.db_keyspace,
        user=args.db_user,
        password=args.db_password
    )

    kafka_client = MyKafkaClient(
        host=args.kafka_host,
        port=args.kafka_port,
        topic=args.kafka_topic,
        msg_s=10
    )

    transaction_rows = cassandra_client.get_transactions(-1)
    final_msgs = []
    for transaction_row in transaction_rows:
        device_resultset = cassandra_client.get_device(transaction_row.account_id, transaction_row.device_hash)
        device_row = device_resultset[0]

        location_resultset = cassandra_client.get_location(transaction_row.account_id, transaction_row.location_hash)
        location_row = location_resultset[0]
        
        msg = get_message_by_db_rows(transaction_row, device_row, location_row)
        final_msgs.append(msg)
    
        kafka_client.send_msg(msg)

    # print(len(final_msgs))


if __name__ == "__main__":
    args = parse_arguments()
    send_db_rows_to_kafka(args)
