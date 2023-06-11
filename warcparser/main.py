from record import WARCRecord
import gzip
import re
import logging
import time
import os
from bs4 import BeautifulSoup
from langdetect import detect_langs
from datetime import timedelta
from urllib.parse import urlparse
from tqdm import tqdm

if __name__ == "__main__":
    filename = "../data/law0.warc"
    file_size = os.path.getsize(filename)

    # Open the file
    start = time.time()

    # Attributes to parse
    warc_re = re.compile(r'warc/0.9')
    http_type_re = re.compile(r'HTTP/1\.(0|1)\s(\w+)')
    
    HTTP_STATUS_CODES = ['301', '302', '403']

    total_records = 0
    _total_records = 0

    with open(filename, 'rb') as f:
        read_bytes = b''

        for i, line in enumerate(tqdm(f, total=os.path.getsize(filename))):
            try:
                if warc_re.search(str(line)):
                    # Strip the header-line
                    header_line = line.decode('utf-8').rstrip()
                    record = WARCRecord(*header_line.split())
                    _total_records += 1

                    path = urlparse(record.subject_uri).path
                    path = path.rsplit('.', 1)[0]
                    path_arr = path.split('/')[1:]

                    if path_arr[0].lower() not in ['', 'base', 'page', 'index', 'home', 'homepage', 'frontpage']:
                        # raise Exception("Likely not an homepage")
                        continue
                    

                    # Read everything up till next record
                    read_bytes = line + f.read(record.data_length - len(line) + 1)

                    m = http_type_re.search(str(read_bytes.decode('utf-8', errors='ignore')))
                    if m is not None:
                        http_status_code = m.group().split()[1]
                        if http_status_code not in HTTP_STATUS_CODES:

                            _, http_header, html = read_bytes.decode('utf-8', errors='ignore').split('\r\n\r\n', 2)
                            try: 
                                soup = BeautifulSoup(html, features="lxml")
                                [s.decompose() for s in soup("script")]  # remove <script> elements
                                body_text = soup.body.get_text()
                                # detect_langs is non-deterministic, must seed first
                                predictions = detect_langs(body_text)
                                for item in predictions:
                                    if item.lang == 'en':
                                        total_records += 1
                                        break
                            except (Exception) as error:
                                # print(http_header)
                                # logging.error(error)
                                continue

            except (Exception) as error:
                logging.error("An error occurred while processing..")
                logging.error(error)

    print("Valid records: {}".format(total_records))
    print("Total records: {}".format(_total_records))
    print("Process time: {}m".format(str(timedelta(seconds=time.time() - start))))