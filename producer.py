import json
import os
import uuid
from datetime import datetime
from random import randint
from time import sleep

import clickhouse_connect
from confluent_kafka import Producer

KAFKA_SERVER = os.environ['KAFKA_SERVER']

CLICKHOUSE_HOST = os.environ['CLICKHOUSE_HOST']
CLICKHOUSE_PORT = os.environ['CLICKHOUSE_PORT']
CLICKHOUSE_USERNAME = os.environ['CLICKHOUSE_USERNAME']
CLICKHOUSE_PASSWORD = os.environ['CLICKHOUSE_PASSWORD']

client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    port=int(CLICKHOUSE_PORT),
    username=CLICKHOUSE_USERNAME,
    password=CLICKHOUSE_PASSWORD)

conf = {'bootstrap.servers': KAFKA_SERVER}
producer = Producer(conf)

widgets = client.query('select workspace_id, dashboard_id, widget_guid from widgets').result_rows
users = client.query('''select distinct user from roles where user != '' ''').result_rows

while True:
    ws, db, widget = widgets[randint(0, len(widgets) - 1)]
    user = users[randint(0, len(users) - 1)][0]
    ts = datetime.now()
    _id = uuid.uuid4()
    _work = uuid.uuid4()
    _dash = uuid.uuid4()
    _wid = uuid.uuid4()

    message = json.dumps({'id': str(_id),
                          'timestamp_utc': str(ts),
                          'user': user,
                          'workspace_id': str(_work),
                          'dashboard_id': str(_dash),
                          'widget_id': str(_wid)})
    producer.produce('log_topic_2', str(message))
    producer.flush()
    sleep(1)
