from datetime import datetime
"""
Working objects with spark requires the classes to be saved in separate files.
"""
class Key:
    def __init__(self, line, qclass_lookup, cameo_lookup, golstein_lookup):
        self.row = line.split(",")
        self.date = datetime.strptime(self.row[0], "%Y-%m") if self.row[0] != 'File count' else datetime.min
        self.pair = (self.row[1], self.row[2]) if self.row[0] != 'File count' else ("", "")
        self.CAMEO = self.row[3] if self.row[0] != 'File count' else -1
        self.count = self.row[4] if self.row[0] != 'File count' else 0

        try:
            self.qclass = qclass_lookup[self.row[3][:2]] if self.row[0] != 'File count' else 'PLACEHOLDER'
            self.description = cameo_lookup[self.row[3]] if self.row[0] != 'File count' else 'PLACEHOLDER'
            self.score = golstein_lookup[self.row[3]] if self.row[0] != 'File count' else 99999
        except KeyError:
            self.score = golstein_lookup[self.row[3][:2]] if self.row[0] != 'File count' else 99999

    def __repr__(self):
        return str({(self.date.strftime("%Y-%m"), self.pair[0], self.pair[1]): (self.score, self.qclass, self.count)})

    def __str__(self):
        return str({(self.date.strftime("%Y-%m"), self.pair[0], self.pair[1]): (self.score, self.qclass, self.count)})
