
from pykafka import KafkaClient
import json


class MyKafkaClient:

    def __init__(self, host, port, topic, msg_s):
        self.client = KafkaClient(hosts="{}:{}".format(host, port))
        self.topic = self.client.topics[topic]

        self.producer = self.topic.get_producer(
            min_queued_messages=msg_s
        )
    
    def send_msg(self, msg):
        data = str.encode(json.dumps(msg))
        self.producer.produce(data)





