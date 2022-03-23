import csv

DELIMITER = ';'
ENCODING = 'ISO-8859-1'
#ENCODING = 'utf-8'

class CsvReader:
    def __init__(self, filename, log=None):
        self.filename = filename
        self.log = log

    def read(self):
        file = open(self.filename, 'r', encoding='utf-8', errors='ignore')
        with file as csvfile:
            for line in csvfile:
                reader = csv.reader([line.replace('\0','')], delimiter=DELIMITER)
                for row in reader:
                    try:
                        yield row
                    except:
                        print(line)
                

    def count_lines(self, chunk_size=65536):
        count = 0
        with open(self.filename, 'r', encoding=ENCODING) as csvfile:
            while True:
                chunk = csvfile.read(chunk_size)
                if not chunk:
                    break
                count += chunk.count('\n')

        return count