"""
    Purpose:
        Kafka Producer Helpers.

        This library is used to aid in creating kafka producers.
"""

# Python Library Imports
import logging
from confluent_kafka import Producer, KafkaException, KafkaError


def get_kafka_producer(kafka_brokers, get_stats=True):
    """
    Purpose:
        Get a Kafka Producer Object (not yet connected to a topic)
    Args:
        kafka_brokers (List of Strings): List of host:port combinations for kakfa brokers
        get_stats (Bool): Whether or not to print statistics. Default is True
    Return:
        kafka_producer (Kafka Producer Obj): Kafka Producer Object
    """
    logging.info(f"Creating Producer for {','.join(kafka_brokers)}")

    producer_configuration = {
        "bootstrap.servers": ",".join(kafka_brokers),
    }

    return Producer(producer_configuration)


def produce_message(kafka_producer, kafka_topic, msg):
    """
    Purpose:
        Consume Kafka Topics
    Args:
        kafka_producer (Kafka Producer Obj): Kafka Producer Object
        kafka_topic (String): Kafka Topic to Produce message to.
        msg (String): Message to produce to Kafka
    Returns:
        N/A
    """
    logging.info(f"Producing Message to Topic {kafka_topic}")

    try:
        kafka_producer.produce(
            kafka_topic, msg, callback=produce_results_callback
        )
    except BufferError as buf_err:
        logging.exception(
            f"Local producer queue is full ({len(kafka_producer)} messages "
            f"awaiting delivery): {buf_err} "
        )
    except Exception as err:
        logging.exception(f"General Kafka Exception During Produce: {err}")


###
# Producer Management, Logging, Callbacks
###


def produce_results_callback(err, msg):
    """
    Purpose:
        Optional per-message delivery callback (triggered by poll() or
        flush()) when a message has been successfully delivered or
        permanently failed delivery (after retries).
    Args:
        err (String): Error Message
        msg (Object): Kafka Callback Message Object
    Return:
        N/A
    """

    if not err:
        logging.info(
            f"Kafka Produce Successful: topic={msg.topic()}, "
            f"partition={msg.partition()}, offset={msg.offset()}"
        )
    else:
        logging.error(f"Kafka Produce Failed: {err}")
