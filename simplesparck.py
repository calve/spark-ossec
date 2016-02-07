"""SimpleApp.py"""
from pyspark import SparkContext

logFile = "file:///userdata/alerts.log"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
logData = sc.textFile(logFile).cache()

alerts_count = logData.filter(lambda s: 'Alert' in s).count()
srcIPs = logData.filter(lambda s: 'Src IP: ' in s).map(lambda s: s[8:])
srcIPs_count = srcIPs.map(lambda s: (s, 1)).reduceByKey(lambda a, b: a + b)
topIPs = srcIPs_count.filter(lambda x: x[1] > 50)
print(topIPs.collect())

rules = logData.filter(lambda s: 'Rule: ' in s)
rules_count = rules.map(lambda s: (s, 1)).reduceByKey(lambda a, b: a + b)
toprules = rules_count.filter(lambda x: x[1] > 50)
print(toprules.collect())


print("Read {0} alerts, with {1} uniques ips".format(alerts_count, srcIPs_count.count()))
