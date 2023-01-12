import random
import copy
import uuid
from datetime import datetime
import numpy as np
import json
from hashlib import sha256
import sys
import time


class LogProducer:

    def __init__(self, max_keys):
        self.max_keys = max_keys

        self.keys = []
        self.data_base = {
            "key": "",
            "content": "",
            "timestamp": 0,
        }
        self.content = [
            "(root) CMD (test -x /etc/pbs_stat.py && /etc/pbs_stat.py cron)",
            "LAuS error - do_command.c:175 - laus_log: (19) laus_log: No such device",
            "unable to qualify my own domain name (sn209) -- using short name"
        ]

    def get_message(self):
        data = copy.deepcopy(self.data_base)

        data["key"] = self.get_key()
        data["content"] = self.get_content()
        data["timestamp"] = self.get_timestamp()

        return data

    def get_key(self):
        if len(self.keys) >= self.max_keys:
            return random.choice(self.keys)
        else:
            key = str(uuid.uuid4())
            self.keys.append(key)
            return key

    def get_content(self):
        return random.choice(self.content)

    def get_timestamp(self):
        if sys.version_info[1] > 6:
            return str(time.time_ns() // 1_000_000)
        else:
            # return str(datetime.now()) # not working in flink deserializer
            return str(int(time.time()) * 1000)

# logProducer = LogProducer(2)
# print(logProducer.get_message())
