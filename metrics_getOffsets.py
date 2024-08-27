import argparse
import confluent_kafka
import hop
import adc
import boto3
from influxdb import InfluxDBClient
import datetime
import json
import os


# GET AWS SECRETS
def get_AWS_secrets(secret_name, region_name):
    """
    This function creates a session with AWS, accesses the Secrets Manager
    service, and retrieves a secret by its name in the specified region.

    Parameters:
    - secret_name (str): The name of the secret in AWS to retrieve.
    - region_name (str): The AWS region name where the secret is stored.

    Returns:
    - secrets
    """
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager',
                            region_name=region_name)
    return client.get_secret_value(SecretId=secret_name)


def parse_kafka_secrets(kafka_secrets_str):
    secrets_dict = {
        key: value.strip('"')
        for key, value in (
            pair.split('=')
            for pair in kafka_secrets_str['SecretString'].split(' ')
        )
    }
    return (secrets_dict['username'], secrets_dict['password'])


def parse_influx_secrets(influx_secrets_str):
    return json.loads(influx_secrets_str['SecretString'])


# HANDLE TERMINAL ARGUMENTS AND SET USERS AND PASSWORDS
parser = argparse.ArgumentParser()
parser.add_argument("--kafka-url",
                    help="URL for connecting to kafka",
                    type=str,
                    default=os.environ.get('KAFKA_URL',
                                           "kafka://kafka.scimma.org"))

parser.add_argument("--kafka-secret-name",
                    help="Name for the secret Kafka credentials",
                    type=str,
                    default=os.environ.get('KAFKA_SECRET',
                                           "prod-kafka-admin-credential"))

parser.add_argument("--influxdb-secret-name",
                    help="Name for the secret InfluxDB credentials",
                    type=str,
                    default=os.environ.get('INFLUXDB_SECRET',
                                           "hop-metrics-influxDB"))

parser.add_argument("--aws-region", help="Name for the AWS region", type=str,
                    default=os.environ.get('AWS_REGION', "us-west-2"))

parser.add_argument("--data-source",
                    help="Discriminate if data comes from Dev or Prod server",
                    type=str,
                    default=os.environ.get('DATA_SOURCE', ""))


args = parser.parse_args()
url = args.kafka_url
kafka_user, kafka_password = parse_kafka_secrets(
    get_AWS_secrets(args.kafka_secret_name, args.aws_region)
)
_, broker_addresses, _ = adc.kafka.parse_kafka_url(url)
data_source = args.data_source


# InfluxDB configuration
influx_secrets = parse_influx_secrets(
    get_AWS_secrets(args.influxdb_secret_name, args.aws_region)
)
influx_host = influx_secrets['host']
influx_port = influx_secrets['port']
influx_user = influx_secrets['username']
influx_password = influx_secrets['password']
influxdb = 'hop_groups_metrics'


# AUTHENTICATE AND CREATE A CONSUMER
auth = hop.auth.Auth(user=kafka_user,
                     password=kafka_password,
                     method=adc.auth.SASLMethod.PLAIN)
conf = adc.consumer.ConsumerConfig(group_id="",
                                   broker_urls=broker_addresses,
                                   auth=auth)
c = adc.consumer.Consumer(conf)._consumer


# INITIALIZE InfluxDB CLIENT
influxdb_client = InfluxDBClient(host=influx_host,
                                 port=influx_port,
                                 username=influx_user,
                                 password=influx_password,
                                 database=influxdb)


# GET THE OFFSETS
meta = c.list_topics()
points = []

for t in meta.topics:
    if t == "__consumer_offsets":
        continue

    group = t.split('.')[0]

    for p in meta.topics[t].partitions:
        # Getting latest offswet for each partition
        tp_max = confluent_kafka.TopicPartition(
            t, p, offset=confluent_kafka.OFFSET_END
        )
        max_result = c.offsets_for_times([tp_max])
        max_off = max_result.pop(0).offset
        time = datetime.datetime.now().isoformat()

        # Storing the offset using an InfluxDB data point
        points.append({
            "measurement": "daily_kafka_offsets",
            "tags": {
                "data_source": data_source,
                "data_point_version": '2',
                "id": str(t)+'-'+str(p)
            },
            "time": time,
            "fields": {
                "group": group,
                "topic": t,
                "partition": p,
                "daily_max_offset": max_off
            }
        })

        print(
            f"Recorded offset for {group}, {t}, partition {p}: {max_off}, "
            f"on date: {time}"
        )

influxdb_client.write_points(points)

influxdb_client.close()
c.close()
