"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging
from configparser import ConfigParser

import requests

logger = logging.getLogger(__name__)


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.info("creating or updating kafka connect connector...")
    kafka_connect_addrs = "http://0.0.0.0:8083" 

    kafka_connect_payload = json.load(open("connector_payload.json"))

    connect_url = f"{kafka_connect_addrs}/connectors/{kafka_connect_payload.get('name')}"
    resp = requests.get(connect_url)
    if resp.status_code == 200:
        logging.debug("connector already created, skipping recreation")
        return

    logger.debug("Kafka connector code working")
    resp = requests.post(
        f"{kafka_connect_addrs}/connectors",
        headers={"Content-Type": "application/json"},
        data=json.dumps(kafka_connect_payload),
    )

    # Ensure a healthy response was given
    resp.raise_for_status()
    logging.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()
