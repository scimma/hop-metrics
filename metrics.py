# IMPORT NECESSARY PACKAGES
import argparse
import confluent_kafka
import hop
import adc
import boto3
import json
import csv


secret_name = "prod-kafka-admin-credential"
region_name = "us-west-2"


# GET AWS SECRETS
def get_AWS_secrets(secret_name, region_name):
    """
    This function creates a session with AWS, accesses the Secrets Manager
    service, and retrieves a secret by its name in the specified region. It
    assumes the secret contains a string with key-value pairs formatted as
    'key=value', separated by spaces. The function parses this string into a
    dictionary, extracting specifically the 'username' and 'password' values,
    and returns them as a tuple.

    Parameters:
    - secret_name (str): The name of the secret in AWS to retrieve.
    - region_name (str): The AWS region name where the secret is stored.

    Returns:
    - tuple: A tuple containing two strings: (username, password).
    """
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager',
                            region_name=region_name)
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name)
    secrets_dict = {key: value.strip('"') for key, value in
                    (pair.split('=') for pair in
                    get_secret_value_response['SecretString'].split(' '))}
    return (secrets_dict['username'], secrets_dict['password'])


# HANDLE TERMINAL ARGUMENTS AND SET USER AND PASSWORD
parser = argparse.ArgumentParser()
parser.add_argument("--kafka-url", help="URL for connecting to kafka", type=str,
                    default="kafka://kafka.scimma.org")
parser.add_argument("--topic", help="Single topic to measure", type=str, default=None)
args = parser.parse_args()

url = args.kafka_url
topic = args.topic
user, password = get_AWS_secrets(secret_name, region_name)
_, broker_addresses, _ = adc.kafka.parse_kafka_url(url)


# AUTHENTICATE AND CREATE A CONSUMER
auth = hop.auth.Auth(user=user, password=password, method=adc.auth.SASLMethod.PLAIN) # USe .PLAIN for admin - SCRAM_SHA_512
conf = adc.consumer.ConsumerConfig(group_id="", broker_urls=broker_addresses, auth=auth)
c = adc.consumer.Consumer(conf)._consumer


#GET RESULTS
result_dict = {}
meta = c.list_topics(topic=topic)
csv_filename = "group_metrics.csv"

print("# topic, messages in buffer, messages ever sent")

with open(csv_filename, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(["topic", "messages_in_buffer", "messages_ever_sent"])
    for t in meta.topics:
        if t == "__consumer_offsets":
            continue
        
        min_query = []
        max_query = []
        buffer_messages = 0
        ever_messages = 0
        
        for p in meta.topics[t].partitions:
            tp_min = confluent_kafka.TopicPartition(t, p, offset=confluent_kafka.OFFSET_BEGINNING)
            tp_max = confluent_kafka.TopicPartition(t, p, offset=confluent_kafka.OFFSET_END)
            min_query.append(tp_min)
            max_query.append(tp_max)
        min_result = c.offsets_for_times(min_query)
        max_result = c.offsets_for_times(max_query)
        
        for p in meta.topics[t].partitions:
            min_off = min_result.pop(0).offset
            max_off = max_result.pop(0).offset
            if max_off > 0:
                ever_messages += max_off
            if max_off > min_off:
                buffer_messages += (max_off - min_off)
        print(t, buffer_messages, ever_messages, flush=True)
        writer.writerow([t, buffer_messages, ever_messages])
        
        # Store results in a dictionary
        group, topic = t.split('.', 1)
        if group not in result_dict:
            result_dict[group] = {"total_historical_msgs": 0, "topics": {}}
        
        result_dict[group]["total_historical_msgs"] += ever_messages
        result_dict[group]["topics"][topic] = buffer_messages

# Sorting results in descending order
sorted_result = sorted(result_dict.items(), key=lambda x: x[1]['total_historical_msgs'], reverse=True)
sorted_dict = {group: data for group, data in sorted_result}


#SAVE TO JSON FILE
# Specify the filename
json_filename = 'groups_metric.json'
# Writing JSON data
with open(json_filename, 'w') as f:
    json.dump(sorted_dict, f, indent=4)


#PRINT A REPORT
print("==============================================")
print("Hopskotch Usage Metrics Per Group (descending)")
print("==============================================")
print()

# Iterate over each group in the sorted dictionary
topic_max_width = max(len(topic) for data in sorted_dict.values() for topic in data["topics"].keys())
for group, data in sorted_dict.items():
    # print(sum(data["topics"].values()))
    last_30_days_total = sum(data["topics"].values())
    print(f"Group: {group}")
    print(f"Total Historical Messages: {data['total_historical_msgs']}")
    print(f"Group's Last 30 Days #Messages: {last_30_days_total}")
    print("Topics:")
    for topic, last_30_days_val in data['topics'].items():
        print(f"\t{group}.{topic:<{topic_max_width}}  |  Last 30 days  #Messages: {last_30_days_val}")
    print("-----------------------------------------------------------------")