from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime
class appLogger:
    def __init__(self):
        cloud_config = {
            'secure_connect_bundle': 'secure-connect-leave-management.zip'
        }
        auth_provider = PlainTextAuthProvider('UKTuZFCtauYBubKfsgTWtGSg',
                                              'ZZH0ATDSD_-.GdSc9EePiyEZxr-yu0XoDD7k04.474W+1IoNjrf-QsYeoHlj7KXk06,QunWxZRi.3oEX0XyaY2YG6.hnMppTHtiH6OyBHu4mRSRsC627Kv9qpPXbWIcs')
        cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
        self.session = cluster.connect()

    def log(self, tag, message):
        table_name= 'db_operation.log'
        timestamp=str(datetime.today().now())[:23]
        query=f"INSERT INTO {table_name} (timestamp, tag, message) VALUES ('{timestamp}', '{tag}', $${message}$$);"
        self.session.execute(query)

