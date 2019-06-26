"""
    Purpose:
        Kafka Topic Helpers.

        This library is used to interact with kafka topics. This includes getting
        a list of the topics, finding details about a topic, creating topics, and
        more.
"""

# Python Library Imports
import logging
from confluent_kafka.admin import NewTopic


###
# Get Information Topics
###


def get_topics(kafka_admin_client, return_system_topics=False):
    """
    Purpose:
        Get a List of Kafka Topics.
    Args:
        kafka_admin_client (Kafka Admin Client Obj): Kafka Admin Client Obj for the
            brokers
    Return:
        kafka_topics (Dict of Kafka Topics): Key is the topic name and value is a
            Kafka metadata object that has basic topic information
    """

    raw_kafka_topics = kafka_admin_client.list_topics().topics

    if return_system_topics:
        kafka_topics = raw_kafka_topics
    else:
        kafka_topics = {}
        for kafka_topic_name, kafka_topic_metadata in raw_kafka_topics.items():
            if kafka_topic_name.startswith("_"):
                continue
            else:
                kafka_topics[kafka_topic_name] = kafka_topic_metadata

    return kafka_topics


###
# Topic Administration
###


def create_kafka_topic(
    kafka_admin_client, topic_name, topic_replication=1, topic_partitions=1
):
    """
    Purpose:
        Create a Kafka Topic
    Args:
        kafka_admin_client (Kafka Admin Client Obj): Kafka Admin Client Obj for the
            brokers
        topic_name (String): Name of the topic to create
        topic_replication (Int): Replication factor for the new topic
        topic_partitions (Int): Number of partitions to devide the topic into
    Return:
        N/A
    """

    kafka_admin_client.create_topics([
        NewTopic(
            topic_name,
            topic_replication,
            topic_partitions,
        )
    ])
