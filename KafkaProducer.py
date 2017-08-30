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
			#print "Kafka Message Send Success!"
		except KafkaError as e:
			print "### Error:{}".format(e)


'''


def send(topic, data):
	producer = Kafka_producer()
	producer.send_data(topic, data)
# for _ in range(10):
#     producer.send_data('_topic', b'fjsafjdsa')
def test():
	pool = multiprocessing.Pool(processes=4)
	for i in range(10):
		producer = Kafka_producer()
		pool.apply_async(producer.send_data,args=("topic_".format(i), "Polly"))
		print "Send {}st".format(i+1)
	pool.close()
	pool.join()

#test()

'''