import argparse
import sys
from libs import json_kafka
from libs import make_data
# main
if __name__ == '__main__':

    # instantiate arg parse
    parser = argparse.ArgumentParser(description='streaming app')

    # add parameters to arg parse
    parser.add_argument('stream', type=str, choices=[
                        'confluent', 'kafka'], help='stream processing')
    parser.add_argument('broker', type=str, help='apache kafka broker')
    parser.add_argument('topic', type=str, help='topic name')
    parser.add_argument('qty_rows', type=int, help='amount of events')

    # invoke help if null
    args = parser.parse_args(args=None if sys.argv[1:] else ['--help'])

    data = make_data.generate_data(args.qty_rows)

    json_kafka.KafkaProducer().json_producer(
        broker=args.broker, topic=args.topic, dataframe=data)

    # cli call
    # python3.8 main.py
    # python3.8 main.py 'kafka' 'dev-kafka-confluent.eastus2.cloudapp.azure.com' 'src-app-music-data-json' 10
    # python3pt.8 main.py 'confluent' 'dev-kafka-confluent.eastus2.cloudapp.azure.com' 'http://dev-kafka-confluent.eastus2.cloudapp.azure.com:8081' 'src-app-music-data-avro' 10
    # python3 ./app/main.py kafka 34.135.189.150:9094 src-app-reservas 200