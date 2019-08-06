from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.protocol import _UNSET_VALUE
from json import loads
import datetime
import time

#  create cluster
#  This will attempt to connection to a Cassandra instance on your local machine (127.0.0.1).
#  You can also specify a list of IP addresses for nodes in your cluster
#  The set of IP addresses we pass to the Cluster is simply an initial set of contact points.
#  After the driver connects to one of these nodes it will automatically discover the rest of the nodes
#  in the cluster and connect to them, so you don’t need to list every node in your cluster.
cluster = Cluster()
#  Instantiating a Cluster does not actually connect us to any nodes.
#  To establish connections and begin executing queries we need a Session,
#  which is created by calling Cluster.connect():
#  The connect() method takes an optional keyspace argument
#  which sets the default keyspace for all queries made through that Session
session = cluster.connect('test')


user_insert = session.prepare("insert into indeed (id, title,category, city,posting,url) values (?,?,?,?,?,?)")
# user_insert_de = session.prepare("insert into indeedde (id, title, posting,url) values (?,?,?,?)")
# user_insert_da = session.prepare("insert into indeedda (id, title, posting,url) values (?,?,?,?)")


consumer = KafkaConsumer(bootstrap_servers=["localhost:9092"] , \
                         auto_offset_reset='earliest',enable_auto_commit=True,group_id="ds"\
                         ,value_deserializer=lambda x: loads(x.decode('utf-8')))
# consumer_de = KafkaConsumer(bootstrap_servers=["localhost:9092"] , \
#                          auto_offset_reset='earliest',enable_auto_commit=True,group_id="de"\
#                          ,value_deserializer=lambda x: loads(x.decode('utf-8')))
# consumer_da = KafkaConsumer(bootstrap_servers=["localhost:9092"] , \
#                          auto_offset_reset='earliest',enable_auto_commit=True,group_id="da"\
#                          ,value_deserializer=lambda x: loads(x.decode('utf-8')))
# consumer.subscribe(['IndeedJobs_Data Scientist','IndeedJobs_Data Engineer','IndeedJobs_Data Analyst'])
consumer.subscribe(['IndeedJobs'])
# consumer_de.subscribe(['IndeedJobs_de'])
# consumer_da.subscribe(['IndeedJobs_da'])


for i,message in enumerate(consumer):
    print(i)
    bound=user_insert.bind([i,message.value['title'],message.value['category'], \
                            message.value['city'],message.value['posting'],message.value['url']])
    session.execute(bound)
#####################
# for i,message in enumerate(consumer_de):
#     print(i)
#     bound_de=user_insert_de.bind([i,message.value['title'],message.value['posting'],message.value['url']])
#     session.execute(bound_de)
# for i, message in enumerate(consumer_da):
#     print(i)
#     bound_da = user_insert_da.bind([i, message.value['title'], message.value['posting'], message.value['url']])
#     session.execute(bound_da)
