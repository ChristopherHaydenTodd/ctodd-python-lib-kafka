#!/usr/bin/env python3
"""
    Purpose:
        Produce to a Kafka Topic

    Steps:
        - Connect to Kafka
        - Create Producer Object
        - Prompt for Input
        - Parse Input
        - Produce Input to Kafka

    example script call:
        python3 produce_to_kafka_topic.py --topic="test-env-topic" \
            --broker="localhost:9092"
"""

# Python Library Imports
import logging
import os
import sys
from argparse import ArgumentParser

# Local Library Imports
from kafka_helpers import kafka_producer_helpers


def main():
    """
    Purpose:
        Produce to a Kafka Topic
    """
    logging.info("Starting Kafka Topic Producing")

    opts = get_options()

    kafka_producer = kafka_producer_helpers.get_kafka_producer(opts.kafka_brokers)
    for user_input in get_input_from_user(max_input=999):
        kafka_producer_helpers.produce_message(
            kafka_producer, opts.kafka_topic, user_input
        )

    logging.info("Kafka Topic Producing Complete")


###
# General/Helper Methods
###


def get_input_from_user(max_input=50):
    """
    Purpose:
        Get input from the user until user exits for max input is reached
    Args:
        N/A
    Yields:
        user_input (String): string input by the user
    """

    end_strings = ('end', 'exit', 'e', 'stop', 'quit', 'q')

    input_number = 0
    while True:
        if input_number >= max_input:
            logging.info(f"Max number of input ({max_input}) has been reached")
            break
        else:
            input_number += 1

        user_input =\
            input(f"Enter Some Data ({','.join(end_strings)} to end): ").rstrip()
        if user_input in end_strings:
            logging.info(f"User has chosen to exit with msg: {user_input}")
            break
        else:
            logging.info(f"Got input from user: {user_input}")
            yield user_input


def get_options():
    """
    Purpose:
        Parse CLI arguments for script
    Args:
        N/A
    Return:
        N/A
    """

    parser = ArgumentParser(description="Produce to Kafka Topic")
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
        "-T", "--topic", "--kafka-topic",
        dest="kafka_topic",
        help="Kafka Topic",
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
        format="[produce_to_kafka_topic] %(asctime)s.%(msecs)03d %(levelname)s %(message)s",
        datefmt="%a, %d %b %Y %H:%M:%S"
    )

    try:
        main()
    except Exception as err:
        logging.exception(
            "{0} failed due to error: {1}".format(os.path.basename(__file__), err)
        )
        raise err
