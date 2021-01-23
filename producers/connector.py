"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging
from pathlib import Path
import requests

logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"
DATABASE = "cta"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logger.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logger.debug("connector already created skipping recreation")
        return

    logger.info("connector code not completed skipping connector creation")
    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "connection.url": f"jdbc:postgresql://localhost:5432/{DATABASE}",
                "connection.user": "cta_admin",
                "connection.password": "chicago",
                "table.whitelist": "stations",
                "mode": "incrementing",
                "incrementing.column.name": "stop_id",
                "topic.prefix": f"{DATABASE}.",
                "batch.max.rows": "500",
                "poll.interval.ms": "8640000",
            }
        }),
    )

    # Ensure a healthy response was given
    resp.raise_for_status()
    logger.debug("connector created successfully")


if __name__ == "__main__":
    configure_connector()