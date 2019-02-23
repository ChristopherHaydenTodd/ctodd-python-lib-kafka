#!/usr/bin/env python3
"""
    Purpose:
        Consume from a Kafka Topic

    Steps:
        - Connect to Kafka
        - Create Consumer Object
        - Poll Topic
        - Parse Message
        - Print Message

    example script call:
        python3 consume_from_kafka_topic.py --topic="test-env-topic" \
            --broker="0.0.0.0:9092" --consumer-group="test-env-consumer"
"""

# Python Library Imports
import logging
import os
import sys
from argparse import ArgumentParser

# Local Library Imports
from kafka_helpers import kafka_consumer_helpers


def main():
    """
    Purpose:
        Consume a Kafka Topic
    """
    logging.info("Starting Kafka Topic Consuming")

    opts = get_options()

    kafka_consumer = kafka_consumer_helpers.get_kafka_consumer(
        opts.kafka_brokers, opts.consumer_group
    )
    kafka_consumer_helpers.consume_topic(kafka_consumer, opts.kafka_topics)

    logging.info("Kafka Topic Consuming Complete")


###
# General/Helper Methods
###


def get_options():
    """
    Purpose:
        Parse CLI arguments for script
    Args:
        N/A
    Return:
        N/A
    """

    parser = ArgumentParser(description="Consume From Kafka Topic")
    required = parser.add_argument_group("Required Arguments")
    optional = parser.add_argument_group("Optional Arguments")

    # Optional Arguments
    # N/A

    # Required Arguments
    required.add_argument(
        "-B", "--broker", "--brokers", "--kafka-broker", "--kafka-brokers",
        action="append",
        dest="kafka_brokers",
        help="Kafka Brokers",
        required=True,
        type=str,
    )
    required.add_argument(
        "-T", "--topic", "--topics", "--kafka-topic", "--kafka-topics",
        action="append",
        dest="kafka_topics",
        help="Kafka Topic",
        required=True,
        type=str,
    )
    required.add_argument(
        "-G", "-g", "-C", "-c", "--group", "--consumer-group",
        dest="consumer_group",
        help="Consumer Group",
        required=True,
        type=str,
    )

    return parser.parse_args()


if __name__ == "__main__":

    log_level = logging.INFO
    logging.getLogger().setLevel(log_level)
    logging.basicConfig(
        stream=sys.stdout,
        level=log_level,
        format="[consume_from_kafka_topic] %(asctime)s %(levelname)s %(message)s",
        datefmt="%a, %d %b %Y %H:%M:%S"
    )

    try:
        main()
    except Exception as err:
        logging.exception(
            "{0} failed due to error: {1}".format(os.path.basename(__file__), err)
        )
        raise err
