
CREATE KEYSPACE master
    WITH REPLICATION = { 
        'class' : 'SimpleStrategy', 
        'replication_factor' : 1
    };

CREATE TABLE master.transactions (
    account_id text, 
    recipient_id text, 
    ts text, 
    result text,
    transaction_id text,
    currency text,
    amount_precision int,
    amount_value int,
    device_hash text,
    location_hash text,
    PRIMARY KEY (account_id, recipient_id, transaction_id)
);

CREATE TABLE master.devices (
    account_id text, 
    hash text,
    os_name text,
    os_version text,
    browser_name text,
    browser_version text,
    timezone text,
    language text,
    PRIMARY KEY (account_id, hash)
);

CREATE TABLE master.locations (
    account_id text, 
    hash text,
    latitude double,
    longitude double,
    altitude double,
    PRIMARY KEY (account_id, hash)
);

CREATE TABLE master.metrics (
    hostname text, 
    throughput_start timestamp,
    throughput_end timestamp,
    throughput int,
    enrichment_type text,
    PRIMARY KEY (hostname, throughput_start, throughput_end, throughput)
);
