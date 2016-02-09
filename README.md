# Spark to Explore data from OSSEC alerts logfile

OSSEC is an HIDS used to secure webservers. It reads application
logfiles to detect suspicious activities.  It produces alerts of the
following format

    ** Alert 1418251171.0: mail  - ossec,
    2014 Dec 10 22:39:31 ip-172-31-36-196->ossec-monitord
    Rule: 502 (level 3) -> 'Ossec server started.'
    ossec: Ossec started.

    ** Alert 1418251186.993: - syslog,sudo
    2014 Dec 10 22:39:46 ip-172-31-36-196->/var/log/auth.log
    Rule: 5402 (level 3) -> 'Successful sudo to ROOT executed'
    User: ubuntu
    Dec 10 22:39:44 ip-172-31-36-196 sudo:   ubuntu : TTY=pts/2 ; PWD=/home/ubuntu ; USER=root ; COMMAND=/usr/bin/tail -f /var/ossec/logs/ossec.log

    ** Alert 1418251186.1306: - pam,syslog,authentication_success,
    2014 Dec 10 22:39:46 ip-172-31-36-196->/var/log/auth.log
    Rule: 5501 (level 3) -> 'Login session opened.'
    Dec 10 22:39:44 ip-172-31-36-196 sudo: pam_unix(sudo:session): session opened for user root by ubuntu(uid=0)

    ** Alert 1418251429.2423: - syslog,sudo
    2014 Dec 10 22:43:49 ip-172-31-36-196->/var/log/auth.log
    Rule: 5402 (level 3) -> 'Successful sudo to ROOT executed'
    User: ubuntu
    Dec 10 22:43:48 ip-172-31-36-196 sudo:   ubuntu : TTY=pts/2 ; PWD=/home/ubuntu ; USER=root ; COMMAND=/usr/bin/tail -f /var/ossec/logs/ossec.log

    ** Alert 1418251429.2737: - pam,syslog,authentication_success,
    2014 Dec 10 22:43:49 ip-172-31-36-196->/var/log/auth.log
    Rule: 5501 (level 3) -> 'Login session opened.'
    Dec 10 22:43:48 ip-172-31-36-196 sudo: pam_unix(sudo:session): session opened for user root by ubuntu(uid=0)

    ** Alert 1418251437.3015: - pam,syslog,
    2014 Dec 10 22:43:57 ip-172-31-36-196->/var/log/auth.log
    Rule: 5502 (level 3) -> 'Login session closed.'
    Dec 10 22:43:55 ip-172-31-36-196 sudo: pam_unix(sudo:session): session closed for user root

    ** Alert 1418251668.3418: - syslog,sshd,
    2014 Dec 10 22:47:48 ip-172-31-36-196->/var/log/auth.log
    Rule: 5702 (level 5) -> 'Reverse lookup error (bad ISP or attack).'
    Src IP: 95.110.225.45
    Dec 10 22:47:48 ip-172-31-36-196 sshd[5404]: reverse mapping checking getaddrinfo for host45-225-110-95.serverdedicati.aruba.it [95.110.225.45] failed - POSSIBLE BREAK-IN ATTEMPT!


We will use Sprak to explore the alerts. As we can see, the alerts
contains a ``rule_id``, and sometimes a `src_ip`. We will use Map/Reduce
which ips generated the more alerts.

## Installation

We'll pull a Docker container to quickly have a working setup.

    $ cd somedirectory/
    $ docker pull sequenceiq/spark:1.6.0
    $ docker run -it -v $PWD:/userdata -p 8088:8088 -p 8042:8042 -h sandbox sequenceiq/spark:1.6.0 bash

We now have a prompt inside our container. We can launch a spark shell
with ``spark`` or ``pyspark``. Also, we exposed the current working
directory to ``/userdata`` so it is now shared between the host and the container.

Opens a newfile called ``simplespark.py``.


## Parsing data

First, we will create a SparkReader to split alerts on paragraph

    from pyspark import SparkContext
    def paragraphFile(sc, path):
        """A custom reader to parse as paragraph"""
        return sc.newAPIHadoopFile(path, "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                                   "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text",
                                   conf={"textinputformat.record.delimiter": "\n\n"}).map(lambda num_line:num_line[1])
    logFile = "file:///userdata/alerts.log"  # Should be some file on your system
    sc = SparkContext("local", "Simple App")
    alerts = paragraphFile(sc, logFile)


Alerts now contains a string representing each paragraph. Let's create a class to parse it in ``alert.py``

    class Alert():
        def __init__(self, raw_alert):
            self.raw_alert = raw_alert.encode('utf8')
            lines = self.raw_alert.split('\n')
            self.src_ip = None
            for line in lines:
                if line.startswith('** Alert'):
                    self.timestamp = float(line.split()[2][:-1])
                if line.startswith('Rule:'):
                    self.rule_id = int(line.split()[1])
                    self.signature = line
                if line.startswith('Src IP:'):
                    self.src_ip = line.split()[2]


We can now use this class to parse our alerts, which will give us actual Python instances.

    from alert import Alert
    alerts = alerts.map(lambda s: Alert(s))
    print("Read {0} alerts".format(alerts.count()))

In the container, you can run your script with the following command
line and see how many alerts were parsed. Look carefully as there can
be a lot of output.

    container$ cd /userdata
    container$ spark-submit simplespark.py --py alert.py


## Find the most active ips

We can now use Spark map/reduce API to count how many alerts were raised by each IP address.

    # Count how much alerts have been raised for each ip
    src_ip_counts = (alerts.filter(lambda alert: alert.src_ip is not None)  # Find Alerts with a src ip
                           .map(lambda alert: (alert.src_ip, 1))  # Map each alert with a constant
                           .reduceByKey(lambda a, b: a+b)  # We actually count here
                           .collect())  # Tell Spark to compute the data


## Find the most proeminent alerts generated by each ips

We can also find which ips generated which kind of alerts.

    # Find all rules id generated by each ips
    alerts_by_ips = (alerts.filter(lambda alert: alert.src_ip is not None)  # alerts with a src ip
                           .map(lambda alert: (alert.src_ip, [alert]))  # Associate ips with a list of rules
                           .reduceByKey(lambda a, b: a+b)
                           .map(lambda alert: (alert, count_in_list(alert[1]))))  # Count each rule occurence
    # Find the most generated alert
    def mapper(item):
        src_ip, alerts = item
        res = []
        for alert, count in alerts.iteritems():
            res.append(((alert, src_ip), count))
        return res

    most_generated = alerts_by_ips.flatMap(mapper).filter(lambda x: x[1] > 200).collect()
    print(most_generated)
