"""Contains functionality related to Lines"""
import json
import logging

from models import Station


logger = logging.getLogger(__name__)


class Line:
    """Defines the Line Model"""

    def __init__(self, color):
        """Creates a line"""
        self.color = color
        self.color_code = "0xFFFFFF"
        if self.color == "blue":
            self.color_code = "#1E90FF"
        elif self.color == "red":
            self.color_code = "#DC143C"
        elif self.color == "green":
            self.color_code = "#32CD32"
        self.stations = {}

    def _handle_station(self, value):
        """Adds the station to this Line's data model"""
        value_json = value if isinstance(value, dict) else json.loads(value)
        if value_json["line"] != self.color:
            return
        self.stations[value_json["station_id"]] = Station.from_message(value_json)

    def _handle_arrival(self, message):
        """Updates train locations"""
        value = message.value() if isinstance(message.value(), dict) else json.loads(message) 
        prev_station_id = value.get("prev_station_id")
        prev_dir = value.get("prev_direction")
        if prev_dir is not None and prev_station_id is not None:
            prev_station = self.stations.get(prev_station_id)
            if prev_station is not None:
                prev_station.handle_departure(prev_dir)
            else:
                logger.debug("unable to handle previous station due to missing station")
        else:
            logger.debug(
                "unable to handle previous station due to missing previous info"
            )

        station_id = value.get("station_id")
        station = self.stations.get(station_id)
        if station is None:
            logger.debug("unable to handle message due to missing station")
            return
        station.handle_arrival(
            value.get("direction"), value.get("train_id"), value.get("train_status")
        )
    
    def _handle_turnstiles(self, message):
            value_json = message.value() if isinstance(message.value(), dict) else json.loads(message) 
            station_id = value_json.get("STATION_ID")
            station = self.stations.get(station_id)
            if station is None:
                logger.debug("unable to handle message due to missing station")
                return
            station.process_message(value_json)

    def process_message(self, message):
        """Given a kafka message, extract data"""
        processors = {
            "^org.chicago.cta.stations.": self._handle_station,
            "^org.chicago.cta.arrivals.": self._handle_arrival,
            "org.chicago.cta.turnstiles": self._handle_turnstiles
        }
        processor = processors.get(message.topic, False)
        if processor:
            processor(message)
        else:
            logger.debug(
                "unable to find handler for message from topic %s", message.topic
            )
