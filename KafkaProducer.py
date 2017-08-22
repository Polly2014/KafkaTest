#!/usr/bin/bin python
# -*- coding: utf-8 -*-
# @Date    : 2017-08-21 22:00:35
# @Author  : Polly
# @Link    : wangbaoli@ict.ac.cn
# @Version : Beta 1.0

from kafka import KafkaProducer
from kafka import KafkaClient
from kafka.errors import KafkaError
import json
import multiprocessing

class Kafka_producer():
	"""docstring for Kafka_producer"""
	def __init__(self, kafkaHost='127.0.0.1', kafkaPort=9092):
		server_str = "{}:{}".format(kafkaHost, kafkaPort)
		self.producer = KafkaProducer(bootstrap_servers=server_str)

	def send_data(self, topic, data):
		try:
			dump_data = json.dumps(data)
			producer = self.producer
			producer.send(topic, dump_data)
			producer.flush()
			print "Message Send Success!"
		except KafkaError as e:
			print "### Error:{}".format(e)


producer = Kafka_producer()
for _ in range(10):
    producer.send_data('_topic', b'fjsafjdsa')

pool = multiprocessing.Pool(processes=4)
for c in col_list:
	pool.apply_async(producer.send_data,args=())
pool.close()
pool.join()


