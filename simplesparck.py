"""SimpleApp.py"""
from pyspark import SparkContext


def paragraphFile(sc, path):
    """A custom reader to parse as paragraph"""
    return sc.newAPIHadoopFile(path, "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                               "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text",
                               conf={"textinputformat.record.delimiter": "\n\n"}).map(lambda num_line:num_line[1])


def mapAlertToValue(alert):
    """
    Parse an alert.
    Returns a (timestamp, rule_id, signature, srcip) tuple.
    """
    timestamp, rule_id, signature, src_ip = 0.0, 0, "", None
    alert = alert.encode('utf8')
    lines = alert.split('\n')
    for line in lines:
        if line.startswith('** Alert'):
            timestamp = float(line.split()[2][:-1])
        if line.startswith('Rule:'):
            rule_id = int(line.split()[1])
            signature = line
        if line.startswith('Src IP:'):
            src_ip = line.split()[2]
    return (timestamp, rule_id, signature, src_ip)


def count_in_list(items):
    """
    Returns a dict counting each item in the list
    Returns {item: count, item: count}
    """
    res = {}
    for item in items:
        if item in res:
            res[item] += 1
        else:
            res[item] = 1
    return res

logFile = "file:///userdata/alerts.log"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
alerts = paragraphFile(sc, logFile).map(mapAlertToValue)

# Count how much alerts have been raised for each ip
src_ip_counts = (alerts.filter(lambda x: x[3] is not None)  # alerts with a src ip
                       .map(lambda s: (s[3], 1))
                       .reduceByKey(lambda a, b: a+b)
                       .collect())

# Find all rules id generated by each ips
rules_by_ips = (alerts.filter(lambda x: x[3] is not None)  # alerts with a src ip
                      .map(lambda s: (s[3], [s[2]]))  # Associate ips with a list of rules
                      .reduceByKey(lambda a, b: a+b)
                      .map(lambda s: (s[0], count_in_list(s[1]))))  # Count each rule occurence

# Find the most generated alert
def mapper(item):
    src_ip, rules = item
    res = []
    for rule, count in rules.iteritems():
        res.append(((rule, src_ip), count))
    return res

most_generated = rules_by_ips.flatMap(mapper).filter(lambda x: x[1] > 200 )
#suspiscious_ips = rules_by_ips.filter(lambda x: len(x[1]) > 1).collect()

#print("Read {0} alerts, with {1} ips".format(alerts.count(), src_ip_counts.count()))
print(most_generated.collect())
