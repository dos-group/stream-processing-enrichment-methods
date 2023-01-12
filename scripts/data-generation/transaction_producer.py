
import random
import copy
import uuid
from datetime import datetime
import numpy as np
import json
from hashlib import sha256
import sys
import time

DATA_BASE = {
    "transaction": {
        "account_id": "", 
        "timestamp": "",
        "result": "SUCCESS/FAILURE",
        "transaction_id": "",
        "recipient_id": "", 
        "amount": {
            "currency": "USD",
            "precision": 2,
            "value": 432,
        },  
    },
    "location": { 
        "hash": "",
        "latitude": 0.0,
        "longitude": 0.0,
        "altitude": 0.0
    },
    "device": {
        "hash": "",
        "os_name": "",
        "os_version": "",
        "browser_name": "",
        "browser_version": "",
        "timezone": "",
        "language": ""
    }
}

"""
key: os name
value: list of versions
"""
OPERATING_SYSTEMS = {
    "macOS": [13, 12, 11], 
    "iOS": [16, 15.6, 12], 
    "Windows": [11, 10, 8, 7], 
    "Android": [12, 11, 10], 
    "Chrome OS": [10, 9, 8], 
    "Linux": [10, 9, 8]
}

"""
key: browser name
value: list of versions
"""
BROWSERS = {
    "Chrome": [10, 9, 8],
    "Safari": [10, 9, 8],
    "Edge": [10, 9, 8],
    "Firefox": [10, 9, 8]
}

TIMEZONES = ["UTC", "GMT", "CET", "MST", "HST"]
LANGUAGES = ["english", "mandarin", "hindi", "spanish", "french", "german"]

# (mean, standard deviation)
AMOUNT_DISTRIBUTIONS = [
    (10, 2), (10, 2), (10, 2), (10, 2), 
    (25, 5), (25, 5),
    (50, 10), (50, 10), 
    (100, 20), 
    (500, 100)
]
MAX_AMOUNT = 1000

"""
key: account id
value: {
    "recipients": []
    "devices": []
    "locations": []
}
"""
account_infos = {}


def get_message(
    existing_account_prob,
    existing_recipient_prob,
    max_devices_per_account,
    new_device_prob,
    max_locations_per_account,
    new_location_prob,
    keep_transaction_in_memory = False,
    transactions_in_memory_num = 10000
):
    data = copy.deepcopy(DATA_BASE)

    data["transaction"]["account_id"] = get_account_id(existing_account_prob)
    data["transaction"]["result"] = get_transaction_result()
    data["transaction"]["transaction_id"] = str(uuid.uuid4())
    data["transaction"]["recipient_id"] = get_recipient_id(data["transaction"]["account_id"], existing_recipient_prob)
    data["transaction"]["amount"] = get_amount()
    data["device"] = get_device(data["transaction"]["account_id"], max_devices_per_account, new_device_prob)
    data["location"] = get_location(data["transaction"]["account_id"], max_locations_per_account, new_location_prob)

    if keep_transaction_in_memory:
        add_transaction_to_account_infos(data, transactions_in_memory_num)

    return data

def get_message_by_db_rows(transaction_row, device_row, location_row):
    data = copy.deepcopy(DATA_BASE)

    if (transaction_row is None or device_row is None or location_row is None):
        return

    data["transaction"]["account_id"] = transaction_row.account_id
    data["transaction"]["timestamp"] = transaction_row.ts
    data["transaction"]["result"] = transaction_row.result
    data["transaction"]["transaction_id"] = transaction_row.transaction_id
    data["transaction"]["recipient_id"] = transaction_row.recipient_id
    data["transaction"]["amount"]["currency"] = transaction_row.currency
    data["transaction"]["amount"]["precision"] = transaction_row.amount_precision
    data["transaction"]["amount"]["value"] = transaction_row.amount_value

    data["location"]["hash"] = transaction_row.location_hash
    data["location"]["latitude"] = location_row.latitude
    data["location"]["longitude"] = location_row.longitude
    data["location"]["altitude"] = location_row.altitude

    data["device"]["hash"] = transaction_row.device_hash
    data["device"]["os_name"] = device_row.os_name
    data["device"]["os_version"] = device_row.os_version
    data["device"]["browser_name"] = device_row.browser_name
    data["device"]["browser_version"] = device_row.browser_version
    data["device"]["timezone"] = device_row.timezone
    data["device"]["language"] = device_row.language

    return data


def get_timestamp():
    if sys.version_info[1] > 6:
        return str(time.time_ns() // 1_000_000)
    else:
        # return str(datetime.now()) # not working in flink deserializer
        return str(int(time.time()) * 1000)


def get_account_info(transaction_row, device_rows, location_rows):
    account_id = transaction_row.account_id
    recipient_id = transaction_row.recipient_id

    recipients = [transaction_row.recipient_id]

    devices = []
    for device_row in device_rows:
        device = {}
        device["os_name"] = device_row.os_name
        device["os_version"] = device_row.os_version
        device["browser_name"] = device_row.browser_name
        device["browser_version"] = device_row.browser_version
        device["timezone"] = device_row.timezone
        device["language"] = device_row.language
        device["hash"] = device_row.hash
        devices.append(device)

    locations = []
    for location_row in location_rows:
        location = {}
        location["latitude"] = location_row.latitude
        location["longitude"] = location_row.longitude
        location["altitude"] = location_row.altitude
        location["hash"] = location_row.hash
        locations.append(location)

    return {
        "recipients": recipients,
        "devices": devices,
        "locations": locations
    }


def update_recipients(account_id, recipient_id):
    if account_id in account_infos and recipient_id not in account_infos[account_id]["recipients"]:
        account_infos[account_id]["recipients"].append(recipient_id)


def add_account_info(account_id, account_info, transactions_in_memory_num):
    if account_id in account_infos:
        return

    while len(account_infos) >= transactions_in_memory_num: 
        random_key = random.choice(list(account_infos.keys()))
        del account_infos[random_key]
    
    account_infos[account_id] = account_info


def add_transaction_to_account_infos(transaction, transactions_in_memory_num):
    while len(account_infos) >= transactions_in_memory_num: 
        random_key = random.choice(list(account_infos.keys()))
        del account_infos[random_key]
    
    account_id = transaction["transaction"]["account_id"]
    recipient_id = transaction["transaction"]["recipient_id"]
    device = transaction["device"]
    location = transaction["location"]

    if account_id not in account_infos:
        account_infos[account_id] = {
            "recipients": [recipient_id],
            "devices": [device],
            "locations": [location]
        }
    elif recipient_id not in account_infos[account_id]["recipients"]:
        account_infos[transaction["transaction"]["account_id"]]["recipients"].append(recipient_id)
    elif device not in account_infos[account_id]["devices"]:
        account_infos[transaction["transaction"]["account_id"]]["devices"].append(device)
    elif location not in account_infos[account_id]["locations"]:
        account_infos[transaction["transaction"]["account_id"]]["locations"].append(location)


def get_account_id(existing_account_prob):
    res = ""

    if random.uniform(0, 1) < existing_account_prob or len(account_infos) == 0:
        # res = str(len(account_infos))
        # res = str(uuid.uuid4())
        res = str(random.randint(0, 5))
    else:
        ids = list(account_infos.keys())
        res = random.choice(ids)
        
    return res


def get_recipient_id(account_id, existing_recipient_prob):
    res = ""

    if (account_id not in account_infos or random.uniform(0, 1) < existing_recipient_prob): 
        res = str(uuid.uuid4())
    else:
        res = random.choice(account_infos[account_id]["recipients"])

    return res


def get_transaction_result():
    if random.uniform(0, 1) < 0.95:
        return "SUCCESS"
    else:
        return "FAILURE"


def get_amount():
    random_idx = random.randint(0, len(AMOUNT_DISTRIBUTIONS) - 1)
    distribution = AMOUNT_DISTRIBUTIONS[random_idx]
    value = np.random.normal(*distribution, 1)[0]

    value = abs(value)
    value = min(value, MAX_AMOUNT)
    # precision 2
    value = int(float("%.2f" % value) * 100)

    return {
        "currency": "USD",
        "precision": 2,
        "value": value
    }


def get_device(account_id, max_devices_per_account, new_device_prob):
    res = {}

    if account_id not in account_infos or (len(account_infos[account_id]["devices"]) < max_devices_per_account and random.uniform(0, 1) < new_device_prob):
        res["os_name"] = random.choice(list(OPERATING_SYSTEMS.keys()))
        res["os_version"] = str(random.choice(OPERATING_SYSTEMS[res["os_name"]]))
        res["browser_name"] = random.choice(list(BROWSERS.keys()))
        res["browser_version"] = str(random.choice(BROWSERS[res["browser_name"]]))
        res["timezone"] = random.choice(TIMEZONES)
        res["language"] = random.choice(LANGUAGES)
        res["hash"] = get_hash(
            res["os_name"] + 
            res["os_version"] + 
            res["browser_name"] + 
            res["browser_version"] + 
            res["timezone"] + 
            res["language"]
        )
    else:
        res = random.choice(account_infos[account_id]["devices"])

    return res


def get_location(account_id, max_locations_per_account, new_location_prob):
    res = {}

    if account_id not in account_infos or (len(account_infos[account_id]["locations"]) < max_locations_per_account and random.uniform(0, 1) < new_location_prob):
        res["latitude"] = random.uniform(0.01, 1.0)
        res["longitude"] = random.uniform(0.01, 1.0)
        res["altitude"] = random.uniform(0.01, 1.0)
        res["hash"] = get_hash(
            str(res["latitude"]) + 
            str(res["longitude"]) + 
            str(res["altitude"])
        )
    else:
        res = random.choice(account_infos[account_id]["locations"])

    return res


def get_hash(input_str):
    return sha256(input_str.encode('utf-8')).hexdigest()


