#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2017-08-21 22:00:35
# @Author  : Polly
# @Link    : wangbaoli@ict.ac.cn
# @Version : Beta 1.0

from _pybgpstream import BGPStream, BGPRecord, BGPElem
from KafkaProducer import Kafka_producer
import multiprocessing
import requests
import json
import time
import sys

NUM_TOTAL = 0

def sendMessageToKafka(col_name, col_data):
    # print "Collector-{} Sending Message...".format(col_name)
    size_total = 0
    num_total = 0
    num_ipv4 = 0
    stream = BGPStream()
    record = BGPRecord()

    time_start = int(col_data.get('updates').get('latestDumpTime'))
    time_end = time_start + int(col_data.get('updates').get('dumpPeriod'))
    # print "Start Time:{}, End Time:{}".format(time_start, time_end)

    stream.add_filter('collector', col_name)
    stream.add_filter('record-type', 'ribs')
    # stream.add_interval_filter(time_start, time_end)
    # stream.add_interval_filter(time_start, time_start+300)
    stream.add_interval_filter(1503475200, 1503475200+7200)
    
    # print "Before Start>>>>>"
    stream.start()
    print col_name
    # print "After Start>>>>>>"
    producer = Kafka_producer()
    while stream.get_next_record(record):
        if record.status=="valid":
            elem = record.get_next_elem()
            while elem:
                if filter(lambda x:':' in x.peer_address, [elem]):
                    num_total += 1
                    elem = record.get_next_elem()
                    continue
                #print "Element:{},{},{}".format(elem.type, elem.peer_address, elem.peer_asn)
                field = elem.fields
                #print type(field)
                prefix = field['prefix'] if field.has_key('prefix') else ''
                next_hop = field['next-hop'] if field.has_key('next-hop') else ''
                as_path = field['as-path'] if field.has_key('as-path') else ''
                as_path = as_path.replace(' ', '|')
                text = [elem.type, elem.peer_address, str(elem.peer_asn), prefix, next_hop, as_path, str(record.time)]
                text = ','.join(text)

                # producer = Kafka_producer()
                producer.send_data(col_name, text)
                num_total += 1
                num_ipv4 += 1
                # print "[{}]-{}".format(col_name, num_total)
                # size_total += len(text)
                #NUM_TOTAL += 1
                #print "[{}]-{}-{}-{}-{}".format(col_name, num_total, num_ipv4, size_total, time.ctime(record.time))
                #print "No.{} Message Send Success-[{}]".format(num_total, text)
                elem = record.get_next_elem()
        else:
            pass
            # print "## Current record not valid!"
            # break
        # print "One Collector Finished"
    else:
        # print "-----------------------------"
        # print "Collector[{}] And Records Send Finished\nTotal Num:{}, IPv4 Num:{}, Total Size:{}".format(col_name, num_total, num_ipv4, size_total)
        # print "-----------------------------"
        print "Collector:[{}]".format(col_name)
        print "Total Num:{}, IPv4 Num:{}, Total Size:{}".format(num_total, num_ipv4, size_total)

class BgpStreamCrawler(object):
    """docstring for BgpStreamCrawler"""
    def __init__(self):
        self.collector_list = []
        self.collector_types = ('routeviews', 'ris')
        self.stream = BGPStream()
        self.record = BGPRecord()

    def getCollectorMetaData(self):
        try:
            r = requests.get("http://bgpstream.caida.org/broker/meta/projects", timeout=2)
            data = json.loads(r.text)['data']['projects']
        except Exception,e:
            return "## Data Get Error:{}-{}".format(Exception, e)
        for col_type, col_data in data.items():
            if col_type in self.collector_types:
                for col_name, col_content in col_data['collectors'].items():
                    col = {
                        "type":col_type, "name":col_name,
                        "ribs":col_content['dataTypes']['ribs'],
                        "updates":col_content['dataTypes']['updates']
                    }
                    self.collector_list.append(col)
        return self.collector_list

    def sendMessageToKafka(self, col_name, col_data):
        print "lalalala"
        stream = BGPStream()
        record = BGPRecord()

        time_start = int(col_data.get('ribs').get('latestDumpTime'))
        time_end = time_start + int(col_data.get('ribs').get('dumpPeriod'))

        stream.add_filter('collector', col_name)
        stream.add_filter('record-type', 'ribs')
        stream.add_interval_filter(time_start, time_end)
        print "Before Start"
        stream.start()
        print "After Start"

        while stream.get_next_record(record):
            if record.status=="valid":
                elem = record.get_next_elem()
                while elem:
                    # print "Record:{}".format(elem)
                    producer = Kafka_producer()
                    producer.send_data(col_name, json.dumps(elem))
                    elem = record.get_next_elem()
            else:
                print "## Current record not valid!"
        print "One Collector Finished"
        

    def sendMessagesToKafka(self, num):
        self.getCollectorMetaData()
        print "Start Send...."
        pool = multiprocessing.Pool(processes=20)
        for collector in [self.collector_list[num]]:
            col_name = collector.get('name')
            # print "Collector Name:{}".format(col_name)
            pool.apply_async(sendMessageToKafka, args=(col_name, collector))
        pool.close()
        pool.join()
        print "All Messages send over!!!"

        



if __name__=="__main__":
    
    bsc = BgpStreamCrawler()
    t0 = time.time()
    bsc.sendMessagesToKafka(int(sys.argv[1]))
    t1 = time.time()
    print "Total time cost:{}s".format(t1-t0)
    #print "Total Num:{}".format(NUM_TOTAL)
    #collector_list = bsc.getCollectorMetaData()
    # pool = multiprocessing.Pool(processes=10)
    # print "Messages Start Send ..."
    # for collector in collector_list:
    #     col_name = collector.get('name')
    #     # print "Collector Name:{}".format(col_name)
    #     pool.apply_async(sendMessageToKafka, args=(col_name, collector))
    
    # pool.close()
    # pool.join()
    # for collector in collector_list:
    #     col_name = collector.get('name')
    #     p = multiprocessing.Process(target=sendMessageToKafka, args=(col_name, collector))
    #     p.start()
    # print "All Messages send over!!!"



#print bgpStreamCrawler.getCollectorMetaData()


#return a list of collector
#collector defination:
#{
# 'type': 'routeviews' or 'ris',
# 'name': specific name of collector,
# 'ribs': {
#     'dumpDuaration': dump duration time,
#     'latestDumpTime':
#     'oldestDumpTime':
#     'dumpPeriod':
#     },
# 'updates':{
#     'dumpDuaration'
#     'latestDumpTime'
#     'oldestDumpTime'
#     'dumpPeriod'
#     }
# }
def getCollectorMetaData():
    try:
        r = requests.get("http://bgpstream.caida.org/broker/meta/projects", timeout=2)
        data = json.loads(r.text)['data']['projects']
    except Exception,e:
        return "## Data Get Error:{}-{}".format(Exception, e)

    col_list = []

    col_types = ('routeviews', 'ris')

    for col_type, col_data in data.items():
        if col_type in col_types:
            for col_name, col_content in col_data['collectors'].items():
                col = {
                    "type":col_type, "name":col_name,
                    "ribs":col_content['dataTypes']['ribs'],
                    "updates":col_content['dataTypes']['updates']
                }
                col_list.append(col)
    return col_list
