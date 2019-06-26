#!/usr/bin/env python3
"""
    Purpose:
        Create a Kafka Topic. Takes in replication and parition information

    Steps:
        - Connect to Kafka
        - Create Kafka Admin Client
        - Create Topic In Kafka

    function call:
        ---
    example script call:
        python3 create_kafka_topic.py --topic-name="test-env-topic" \
            --topic-replication=3 --topic-partitions=4 \
            --broker="localhost:9092"
"""

# Python Library Imports
import logging
import os
import sys
from argparse import ArgumentParser

# Local Library Imports
from kafka_helpers import kafka_admin_helpers, kafka_topic_helpers


def main():
    """
    Purpose:
        Produce to a Kafka Topic
    """
    logging.info("Starting Kafka Topic Creation")

    opts = get_options()

    kafka_admin_client = kafka_admin_helpers.get_kafka_admin_client(opts.kafka_brokers)
    kafka_topics = kafka_topic_helpers.get_topics(kafka_admin_client)

    if opts.topic_name in kafka_topics:
        error_msg = f"Topic name already exists: {opts.topic_name}"
        raise Exception(error_msg)

    kafka_topic_helpers.create_kafka_topic(
        kafka_admin_client,
        opts.topic_name,
        topic_replication=opts.topic_replication,
        topic_partitions=opts.topic_partitions
    )

    logging.info("Kafka Topic Creation Complete")


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
    optional.add_argument(
        "-R", "--replication", "--topic-replication",
        dest="topic_replication",
        help="Replication factor of the topic to create",
        required=False,
        default=1,
        type=int,
    )
    optional.add_argument(
        "-P", "--partitions", "--topic-partitions",
        dest="topic_partitions",
        help="Number of partitions of the topic to create",
        required=False,
        default=1,
        type=int,
    )

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
        "-T", "--topic", "--kafka-topic", "--topic-name",
        dest="topic_name",
        help="Topic name to create",
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
        format="[create_kafka_topic] %(asctime)s.%(msecs)03d %(levelname)s %(message)s",
        datefmt="%a, %d %b %Y %H:%M:%S"
    )

    try:
        main()
    except Exception as err:
        logging.exception(
            "{0} failed due to error: {1}".format(os.path.basename(__file__), err)
        )
        raise err
