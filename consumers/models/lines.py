"""Contains functionality related to Lines"""
import json
import logging

from models import Line


logger = logging.getLogger(__name__)

TOPIC_STATION="com.udacity.ducnv104.project1.cta.stations"
TOPIC_ARRIVAL_TRAIN_PREFIX="com.udacity.ducnv104.project1.train_arrival"
TOPIC_TURNSTILE="com.udacity.ducnv104.project1.turnstile"


class Lines:
    """Contains all train lines"""

    def __init__(self):
        """Creates the Lines object"""
        self.red_line = Line("red")
        self.green_line = Line("green")
        self.blue_line = Line("blue")

    def process_message(self, message):
        """Processes a station message"""
        if "com.udacity.ducnv104.project1" in message.topic() and "weather" not in message.topic():
        # if "org.chicago.cta.station" in message.topic():
            value = message.value()
            if message.topic() == TOPIC_STATION: 
            # if message.topic() == "org.chicago.cta.stations.table.v1":
                value = json.loads(value)
            if value["line"] == "green":
                self.green_line.process_message(message)
            elif value["line"] == "red":
                self.red_line.process_message(message)
            elif value["line"] == "blue":
                self.blue_line.process_message(message)
            else:
                logger.debug("discarding unknown line msg %s", value["line"])
        elif "TURNSTILE_SUMMARY" == message.topic():
            self.green_line.process_message(message)
            self.red_line.process_message(message)
            self.blue_line.process_message(message)
        else:
            logger.info("ignoring non-lines message %s", message.topic())
