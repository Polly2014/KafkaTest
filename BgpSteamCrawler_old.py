#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2017-08-21 22:00:35
# @Author  : Polly
# @Link    : wangbaoli@ict.ac.cn
# @Version : Beta 1.0

from _pybgpstream import BGPStream, BGPRecord, BGPElem
import json

stream = BGPStream()
rec = BGPRecord()

#stream.add_filter('collector','rrc11')
stream.add_interval_filter(1438417216,1438417216)

stream.start()

while(stream.get_next_record(rec)):
    if rec.status != "valid":
        print rec.project, rec.collector, rec.type, rec.time, rec.status
    else:
        elem = rec.get_next_elem()
        while(elem):
            # print rec.project, rec.collector, rec.type, rec.time, rec.status,
            # print elem.type, elem.peer_address, elem.peer_asn, elem.fields
            print "{rec.project} {rec.collector} {rec.type} {rec.time} {rec.status}".format(rec=rec)
            print elem
            print "-------------------------"
            elem = rec.get_next_elem()
