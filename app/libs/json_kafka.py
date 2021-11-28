# import libraries
import json
from confluent_kafka import Producer


class KafkaProducer():

    def delivery_report(self, err, msg):
        """called once for each message produced to indicate delivery result. triggered by poll() or flush()."""
        if err is not None:
            print('message delivery failed: {}'.format(err))
        else:
            print('message successfully produced to {} [{}] at offset {}'.format(
                msg.topic(), msg.partition(), msg.offset()))

    # get data to insert from (read_files.csv)
    # print(type(CSV().csv_reader()))
    # <class 'list'>

    def json_producer(self, broker, topic, dataframe):
        """responsible to send data to kafka in json using utf-8"""

        # producer configuration
        p = Producer({
            'client.id': 'json-python-stream-app',
            'bootstrap.servers': broker,
            "batch.num.messages": 100,
            "queue.buffering.max.ms": 100,
            "queue.buffering.max.messages": 1000,
            "linger.ms": 200
        })

        # get data to be inserted
        get_data = dataframe.to_dict(orient='records')

        # for loop to insert all data
        for data in get_data:

            try:
                # trigger any available delivery report callbacks from previous produce() calls
                p.poll(0)

                # asynchronously produce a message, the delivery report callback
                # will be triggered from poll() above, or flush() below, when the message has
                # been successfully delivered or failed permanently.
                p.produce(
                    topic=topic,
                    value=json.dumps(data, default=str).encode('utf-8'),
                    callback=self.delivery_report
                )

            except BufferError:
                print("buffer full")
                p.poll(0.1)

            except ValueError:
                print("invalid input")
                raise

            except KeyboardInterrupt:
                raise

        # wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
        p.flush()
