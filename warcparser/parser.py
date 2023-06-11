from record import WARCRecord
import gzip
import os
import re
import logging
from tqdm import tqdm
from urllib.parse import urlparse

if __name__ == "__main__":

    file_name = "../data/law0.warc.gz"
    file_size = os.stat(file_name).st_size

    warc_re = re.compile(r'warc/0.9')
    http_re = re.compile(r'HTTP/1\.(0|1)\s[0-9]{3}\s(\w+)')
    
    # HTTP_STATUS_CODES = ['301', '302', '403']

    total_records = 0

    with tqdm(total=file_size, unit_scale=True, unit_divisor=1024, unit="B") as pbar:
        with gzip.open(file_name, 'r') as warc_file:
            
            record = None
            body = b""

            for line in warc_file:
                try:
                    if warc_re.search(str(line)):
                        if record is not None:
                            total_records += 1

                            # print(str(body.decode('utf-8', errors="ignore")))
                            record = None
                            body = b""
                        else: 
                            record = WARCRecord(*(str(line).split()))

                            # path = urlparse(record.subject_uri).path
                            # if path.rsplit('.', 1)['/', '/index', '/home', '/homepage']:
                            #     print(path)
                            #     record = None
                    elif record is not None:
                        body += line
                        # Throw away HTTP with Status Code 3XX or 4XX
                        if http_re.search(str(line)):
                            http_status = str(line)[11:14].rstrip()
                            if http_status != '200':
                                record = None
                                body = b""
                except (Exception) as error:
                    logging.error("An error occurred while processing..")
                    logging.error(error)
                
                pbar.update(len(line))
            
            # Do something with last record
        
print(f'Total Number of Records in the file is {total_records}')