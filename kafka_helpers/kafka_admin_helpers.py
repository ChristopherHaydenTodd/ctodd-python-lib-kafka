"""
    Purpose:
        Kafka Admin Helpers.

        This library is used to interacting with Kafka Admin functionality. This
        includes getting the admin object that will return details about kafka
        state.
"""

# Python Library Imports
import logging
from confluent_kafka.admin import AdminClient


###
# Admin Helpers
###


def get_kafka_admin_client(kafka_brokers):
    """
    Purpose:
        Get a Kafka Admin Client Object. Allows for polling information about Kafka
        configuration and creating objects in Kafka
    Args:
        kafka_brokers (List of Strings): List of host:port combinations for kakfa
            brokers
    Return:
        kafka_admin_client (Kafka Admin Client Obj): Kafka Admin Client Obj for the
            brokers
    """
    logging.info(f"Getting Admin for {','.join(kafka_brokers)}")

    kafka_configuration = {
        "bootstrap.servers": ",".join(kafka_brokers),
    }

    return AdminClient(kafka_configuration)
