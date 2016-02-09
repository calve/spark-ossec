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
