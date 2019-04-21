"""
    Purpose:
        Kafka Consumer Helpers.

        This library is used to aid in creating kafka consumers.
"""

# Python Library Imports
import logging
import simplejson as json
from confluent_kafka import Consumer, KafkaException, KafkaError


def get_kafka_consumer(
    kafka_brokers,
    consumer_group="default",
    timeout=6000,
    offset_start="latest",
    get_stats=True
):
    """
    Purpose:
        Get a Kafka Consumer Object (not yet connected to a topic)
    Args:
        kafka_brokers (List of Strings): List of host:port combinations for kakfa brokers
        consumer_group (String): Consumer group to consume as. default is "default"
        timeout (String): Timeout in ms if no messages are found (during poll). Default
            is 6000
        offset_start (String): Where to start consuming with respect to the consumer
            group/topic offset. Default is "latest", which ignores any messages in the
            topic before the consumer begins consuming
        get_stats (Bool): Whether or not to print statistics. Default is True
    Return:
        kafka_consumer (Kafka Consumer Obj): Kafka Consumer Object
    """
    logging.info(f"Creating Consumer {consumer_group} for {','.join(kafka_brokers)}")

    consumer_configuration = {
        "bootstrap.servers": ",".join(kafka_brokers),
        "group.id": consumer_group,
        "session.timeout.ms": timeout,
        "auto.offset.reset": offset_start,
    }

    if get_stats:
        consumer_configuration["statistics.interval.ms"] = 100000
        consumer_configuration["stats_cb"] = consumer_statistic_callback

    consumer_logger = get_consumer_logger(consumer_group)

    return Consumer(consumer_configuration, logger=consumer_logger)


def consume_topic(kafka_consumer, kafka_topics):
    """
    Purpose:
        Consume Kafka Topics
    Args:
        kafka_consumer (Kafka Consumer Obj): Kafka Consumer Object
        kafka_topics (List of Strings): List of Kafka Topics to Consume.
    Yields:
        msg (Kafka Message Obj): Message Obj returned from the topic
    """
    logging.info(f"Consuming Topics {', '.join(kafka_topics)}")

    # Subscribe to topics
    kafka_consumer.subscribe(kafka_topics, on_assign=consumer_assignment_callback)

    # Read messages from Kafka, print to stdout
    try:
        while True:
            msg = kafka_consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logging.info(
                        'Reached End of Offset: topic={0}, '
                        'partition={1}, offset={2}'.format(
                            msg.topic(), msg.partition(), msg.offset()
                        )
                    )
                else:
                    raise KafkaException(msg.error())
            else:
                logging.info(
                    'Got Message from Topic: topic={0}, partition={1}, '
                    'offset={2}, key={3}, value={4}'.format(
                        msg.topic(), msg.partition(),
                        msg.offset(), msg.key(), msg.value()
                    )
                )
                print(
                    'Key {0} Returned {1}'.format(
                        msg.key(),
                        int.from_bytes(msg.value(), byteorder='big')
                    )
                )
    except KeyboardInterrupt:
        logging.info('Consume Ended By User')
    except KafkaException as err:
        logging.error('KafkaException Raise: {0}'.format(err))
    finally:
        kafka_consumer.close()


###
# Consumer Management, Logging, Callbacks
###


def consumer_assignment_callback(consumer, partitions):
    """

    """

    print('Assignment:', partitions)


def consumer_statistic_callback(stats_json_str):
    """
    Purpose:
        Parse CLI arguments for script
    Args:
        brokers (List of Strings): List of host:port combinations for kakfa topics
        consumer_group (String): Something
        timeout (String): Something
        offset_start (String): Something
    Return:
        kafka_consumer (Kafka Consumer Obj): Kafka Consumer Object
    """

    return json.loads(stats_json_str)


def get_consumer_logger(logger_name="consumer", log_level=logging.INFO):
    """
    Purpose:
        Parse CLI arguments for script
    Args:
        brokers (List of Strings): List of host:port combinations for kakfa topics
        consumer_group (String): Something
        timeout (String): Something
        offset_start (String): Something
    Return:
        kafka_consumer (Kafka Consumer Obj): Kafka Consumer Object
    """

    # Create logger for consumer (logs will be emitted when poll() is called)
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)-15s %(levelname)-8s %(message)s")
    )
    logger.addHandler(handler)

    return logger
