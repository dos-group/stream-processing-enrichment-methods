
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import *


TRANSACTION_QUERY = "INSERT INTO transactions (account_id, recipient_id, ts, result, transaction_id, currency, amount_precision, amount_value, device_hash, location_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
DEVICE_QUERY = "INSERT INTO devices (account_id, hash, os_name, os_version, browser_name, browser_version, timezone, language) VALUES (?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS"
LOCATION_QUERY = "INSERT INTO locations (account_id, hash, latitude, longitude, altitude) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS"
METRICS_QUERY = "INSERT INTO metrics (hostname, throughput_start, throughput_end, throughput, enrichment_type) VALUES (?, ?, ?, ?, ?)"

SELECT_TRANSACTIONS_LIMIT_QUERY = "SELECT * FROM transactions LIMIT {}"
SELECT_TRANSACTIONS_QUERY = "SELECT * FROM transactions"

SELECT_DEVICES_QUERY = "SELECT * FROM devices WHERE account_id = '{}'"
SELECT_DEVICE_QUERY = "SELECT * FROM devices WHERE account_id = '{}' and hash = '{}'"

SELECT_LOCATIONS_QUERY = "SELECT * FROM locations WHERE account_id = '{}'"
SELECT_LOCATION_QUERY = "SELECT * FROM locations WHERE account_id = '{}' and hash = '{}'"

class CassandraTransactionClient:

    def __init__(self, host, port, keyspace, user, password):
        auth_provider = PlainTextAuthProvider(
            username=user, 
            password=password
        )
        cluster = Cluster(
            contact_points=[host], 
            port=port, 
            auth_provider=auth_provider
        )
        self.session = cluster.connect(keyspace)
        self.batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)

        self.insert_transactions_query = self.session.prepare(TRANSACTION_QUERY)
        self.insert_devices_query = self.session.prepare(DEVICE_QUERY)
        self.insert_locations_query = self.session.prepare(LOCATION_QUERY)
        self.insert_metrics_query = self.session.prepare(METRICS_QUERY)
    
    def get_transactions(self, limit):
        if limit > 0:
            return self.session.execute(SELECT_TRANSACTIONS_LIMIT_QUERY.format(limit))
        else:
            return self.session.execute(SELECT_TRANSACTIONS_QUERY)
    
    def get_devices(self, account_id):
        return self.session.execute(SELECT_DEVICES_QUERY.format(account_id))
    
    def get_device(self, account_id, device_hash):
        return self.session.execute(SELECT_DEVICE_QUERY.format(account_id, device_hash))

    def get_locations(self, account_id):
        return self.session.execute(SELECT_LOCATIONS_QUERY.format(account_id))
    
    def get_location(self, account_id, location_hash):
        return self.session.execute(SELECT_LOCATION_QUERY.format(account_id, location_hash))

    def insert_metrics(self, hostname, throughput_start, throughput_end, throughput, enrichment_type = 'nd'):
        self.session.execute(
            self.insert_metrics_query,
            [
                hostname, 
                throughput_start,
                throughput_end,
                throughput,
                enrichment_type
            ]
        )
    
    def insert_transaction_batch(self, transactions):
        for t in transactions:
            self.batch.add(
                self.insert_transactions_query, 
                (
                    t["transaction"]["account_id"], 
                    t["transaction"]["recipient_id"], 
                    t["transaction"]["timestamp"], 
                    t["transaction"]["result"], 
                    t["transaction"]["transaction_id"], 
                    t["transaction"]["amount"]["currency"], 
                    t["transaction"]["amount"]["precision"], 
                    t["transaction"]["amount"]["value"], 
                    t["device"]["hash"], 
                    t["location"]["hash"]
                )
            )
        self.session.execute(self.batch)
        self.batch.clear()

    def insert_device(self, transaction):
        self.session.execute(
            self.insert_devices_query, 
            [
                transaction["transaction"]["account_id"],
                transaction["device"]["hash"],
                transaction["device"]["os_name"],
                transaction["device"]["os_version"],
                transaction["device"]["browser_name"],
                transaction["device"]["browser_version"],
                transaction["device"]["timezone"],
                transaction["device"]["language"]
            ]
        )

    def insert_location(self, transaction):
        self.session.execute(
            self.insert_locations_query,
            [
                transaction["transaction"]["account_id"],
                transaction["location"]["hash"],
                transaction["location"]["latitude"],
                transaction["location"]["longitude"],
                transaction["location"]["altitude"]
            ]
        )

