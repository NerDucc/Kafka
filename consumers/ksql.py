"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

#
# TODO: Complete the following KSQL statements.
# TODO: For the first statement, create a `turnstile` table from your turnstile topic.
#       Make sure to use 'avro' datatype!
# TODO: For the second statment, create a `turnstile_summary` table by selecting from the
#       `turnstile` table and grouping on station_id.
#       Make sure to cast the COUNT of station id to `count`
#       Make sure to set the value format to JSON

KSQL_STATEMENT_ORIGIN = """
CREATE TABLE turnstile (
    station_id varchar,
    station_name varchar,
    line varchar
) WITH (
    KEY='station_id'.
    KAFKA_TOPIC='com.udacity.ducnv104.project1.turnstile',
    VALUE_FORMAT='AVRO'
) 

CREATE TABLE turnstile_summary WITH(
    VALUE_FORMAT='JSON',
    KAFKA_TOPIC='TURNSTILE_SUMMARY',
    KEY='station_id'
)
SELECT
    station_id,
    count(*) AS COUNT
FROM turnstile
GROUP BY station_id
"""

# I think using stream to load data from station id is better
# If I use station_id as key and use table to load data from stream --> station_id could be override
# So to keep all data, I use stream instead
KSQL_STATEMENT = """
CREATE STREAM turnstile_stream (
    station_id varchar,
    station_name varchar,
    line varchar
)
WITH (
    KAFKA_TOPIC='com.udacity.ducnv104.project1.turnstile',
    VALUE_FORMAT='AVRO',
    KEY='station_id'
);
    

CREATE TABLE turnstile_summary WITH (
    VALUE_FORMAT='JSON',
    KAFKA_TOPIC='TURNSTILE_SUMMARY'
) AS
SELECT
    station_id,
    count(*) AS COUNT
FROM turnstile_stream
WINDOW SESSION (20 MINUTES)
GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")
    ksql_data = json.dumps(
        {
            "ksql": KSQL_STATEMENT,
            "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
        }
    )
    print(ksql_data)

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=ksql_data,
    )

    # Ensure that a 2XX status code was returned
    try:
        ## Ensure a healthy response was given
        resp.raise_for_status()
        logging.debug("KSQL ran successfully")
    except requests.HTTPError as e:
        logger.error(f"Error running KSQ TURNSTILE_SUMMARY: {str(e)}")
        raise


if __name__ == "__main__":
    execute_statement()
