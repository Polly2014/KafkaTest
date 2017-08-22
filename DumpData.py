from _pybgpstream import BGPStream, BGPElem, BGPRecord
import DataProviderCrawler
import csv

collectors = DataProviderCrawler.getCollectorMetaData()
print 'Got ' + str(len(collectors)) + ' collectors: '
for c in collectors:
    print c['name']

def isIPV6(elem):
    f = elem.peer_address
    return (len(f.split(':')) > 1)

def dump2file(type, name, latestDumpTime, dumpDuration, dumpPeriod):
    print type + ' of ' + name + ': '
    csv_header = ['type', 'addr', 'as', 'prefix', 'next_hop', 'as_path']
    _file = None
    stream = BGPStream()
    rec = BGPRecord()
    stream.add_filter('collector', c_name)
    if type == 'ribs':
        stream.add_filter('record-type', 'ribs')
        _file = open(c_name + '_ribs.csv', 'w+')
    elif type == 'updates':
        stream.add_filter('record-type', 'updates')
        _file = open(c_name + '_updates.csv', 'w+')
    stream.add_interval_filter(latestDumpTime, latestDumpTime + dumpPeriod)

    stream.start()

    count = 0
    useless_c = 0

    writer = csv.writer(_file)
    writer.writerow(csv_header)

    # Get next record
    while (stream.get_next_record(rec)):
        # Print the record information only if it is not a valid record
        if rec.status != "valid":
            # print rec.project, rec.collector, rec.type, rec.time, rec.status
            print 'current rec not valid.'
        else:
            elem = rec.get_next_elem()
            while (elem):
                useless_c += 1
                if useless_c % 1000 == 0:
                    print 'Got ' + str(useless_c) + ' elem totally.'
                # Print record and elem information
                if isIPV6(elem):  # ipv6 packet ignored
                    elem = rec.get_next_elem()
                    continue
                count += 1
                # print rec.project, rec.collector, rec.type, rec.time, rec.status,
                # print elem.type, elem.peer_address, elem.peer_asn
                # elem.fields contains four column: communities, next-hop, prefix, as-path
                field = elem.fields
                prefix = field['prefix'] if 'prefix' in field.keys() else ''
                next_hop = field['next-hop'] if 'next-hop' in field.keys() else ''
                as_path = field['as-path'] if 'as-path' in field.keys() else ''
                as_path = as_path.replace(' ', '|')

                writer.writerow([elem.type, elem.peer_address, elem.peer_asn, prefix, next_hop, as_path])

                elem = rec.get_next_elem()
    _file.close()
    print 'count: ' + str(count)
    return count


_f = open('stats.txt', 'w+')
_w = csv.writer(_f)
_w.writerow(['collector', 'updates', 'ribs'])

for collector in collectors[:19]:
    c_name = collector['name']
    c_type = collector['type']
    c_updates = collector['updates']
    c_ribs = collector['ribs']

    u_latestDumpTime = int(c_updates['latestDumpTime'])
    u_dumpDuration = int(c_updates['dumpDuration'])
    u_dumpPeriod = int(c_updates['dumpPeriod'])

    r_latestDumpTime = int(c_ribs['latestDumpTime'])
    r_dumpDuration = int(c_ribs['dumpDuration'])
    r_dumpPeriod = int(c_ribs['dumpPeriod'])

    u_count = dump2file('updates', c_name, u_latestDumpTime, u_dumpDuration, u_dumpPeriod)
    r_count = dump2file('ribs', c_name, r_latestDumpTime, r_dumpDuration, r_dumpPeriod)
    _w.writerow([c_name, u_count, r_count])
_f.close()