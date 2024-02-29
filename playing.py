import argparse
# import os
# from datetime import datetime
import time
import confluent_kafka
import hop
from hop import Stream
import adc


parser = argparse.ArgumentParser()
parser.add_argument("--kafka-url", help="URL for connecting to kafka", type=str,
					default="kafka://kafka.scimma.org")
parser.add_argument("--topic", help="Single topic to measure", type=str, default=None)
args = parser.parse_args()

url = args.kafka_url
topic = args.topic
# user = os.environ.get("KAFKA_USER")
# password = os.environ.get("KAFKA_PASSWORD")
user = "fabs-7531f8ee"
password = "gO2EQlQoyzzfs2TBSCcmCs0GNlWiy23m"

urluser, broker_addresses, topics = adc.kafka.parse_kafka_url(url)
if urluser: # allow explicit user name to override environment variable
    user = urluser

auth = hop.auth.Auth(user=user, password=password, method=adc.auth.SASLMethod.SCRAM_SHA_512)
conf = adc.consumer.ConsumerConfig(group_id="", broker_urls=broker_addresses, auth=auth)
c = adc.consumer.Consumer(conf)._consumer

c.Stream()

result_dict = {}
meta = c.list_topics(topic=topic)

# print("# topic, messages in buffer, messages ever sent")
for t in meta.topics:
    if t == "__consumer_offsets":
        continue
    
    # with hop.Stream().open("kafka://kafka.scimma.org/"+t, "r") as s:
    #     for message, metadata in s.read(metadata=True):
    #         nowFloat = time.time()
    #         now_ms = int(nowFloat * 10**3)
    #         print("Kafka time:", metadata.timestamp,
    #               "Receipt time:", now_ms,
    #               "Transit time:", (now_ms - metadata.timestamp),"ms")
    # print(t)
    
    # min_query = []
    # max_query = []
    # buffer_messages = 0
    # ever_messages = 0
    
    # for p in meta.topics[t].partitions:
    #     tp_min = confluent_kafka.TopicPartition(t, p, offset=confluent_kafka.OFFSET_BEGINNING)
    #     tp_max = confluent_kafka.TopicPartition(t, p, offset=confluent_kafka.OFFSET_END)
    #     min_query.append(tp_min)
    #     max_query.append(tp_max)
    # min_result = c.offsets_for_times(min_query)
    # max_result = c.offsets_for_times(max_query)
    
    # for p in meta.topics[t].partitions:
    #     min_off = min_result.pop(0).offset
    #     max_off = max_result.pop(0).offset
    #     if max_off > 0:
    #         ever_messages += max_off
    #     if max_off > min_off:
    #         buffer_messages += (max_off - min_off)
    # print(t, sum, ever_messages, flush=True)