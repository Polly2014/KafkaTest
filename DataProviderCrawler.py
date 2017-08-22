#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2017-08-21 22:00:35
# @Author  : Polly
# @Link    : wangbaoli@ict.ac.cn
# @Version : Beta 1.0

import requests
import json
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

print getCollectorMetaData()