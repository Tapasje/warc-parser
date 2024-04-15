import gzip
import os
import logging
import csv
import io

from record import WARCRecord
from tqdm import tqdm
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from langdetect import DetectorFactory, detect
from urllib.parse import urlparse
from dataprocessor import DataProcessor

DetectorFactory.seed = 42


def write_to_csv(file_path, text, label):
    # Function to write a new row to the CSV file
    with open(file_path, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow([text, label])


def extract_html(byte_list):
    response_index = -1
    for idx, line in enumerate(byte_list):
        if line.startswith(b'HTTP'):
            if b'200' in line:
                response_index = idx
                break
    if response_index == -1:
        return ""

    start_index = -1
    end_index = -1
    byte_list = byte_list[response_index + 1:]

    for idx, line in enumerate(byte_list):
        if line.startswith(b'<!DOCTYPE'):
            start_index = idx
        if line.startswith(b'</html>'):
            end_index = idx
        if start_index != -1 and end_index != -1:
            break

    if start_index != -1 and end_index != -1:
        html_content = b''.join(byte_list[start_index:end_index]).decode(
            'utf-8', errors="ignore")
    else:
        html_content = ""

    return html_content


def get_homepage(subject_uri: str):
    parsed_url = urlparse(subject_uri)
    domain = parsed_url.netloc.lower()
    path = parsed_url.path.lower()

    if path == '/' or not path:
        return '/index.html', domain

    _, file = os.path.split(parsed_url.path.lower())

    if file.startswith(('/index.', '/default.', '/home.', 'homepage.', '/main.', '/welcome.')):
        return path, domain

    return None, None

def main(data_processor: DataProcessor = DataProcessor('../data/labels/webspam-uk2007-set1-1.0/WEBSPAM-UK2007-SET1-labels.txt',
    '../data/labels/webspam-uk2007-set1-1.0/WEBSPAM-UK2007-hostnames.txt',
    '../data/processed/')) -> None:
    
    for i in range(8):
        file_path = f"../data/raw/law{i}.warc"
        file_size = os.stat(file_path).st_size

        print(f"Now processing: {file_path}")
        with tqdm(total=file_size, unit_scale=True, unit_divisor=1024, unit="B") as pbar:
            with open(file_path, mode="rb") as f:
                record = None
                byte_lst = []

                for line in f:
                    pbar.update(len(line))

                    if line.startswith(b'warc/0.9'):
                        if not record:
                            record = WARCRecord(*(str(line).split()))

                            parsed_url = urlparse(record.subject_uri)
                            if not data_processor.is_hostname_in_dict(parsed_url.hostname):
                                record = None
                        else: 
                            try:
                                path, domain = get_homepage(record.subject_uri)
                                if path and domain:
                                    html_content = extract_html(byte_lst)
                                    soup = BeautifulSoup(html_content, features='html.parser')

                                    # Check if the page is in English
                                    text = ''.join(soup.stripped_strings)
                                    if detect(text) == 'en':
                                        parsed_url = urlparse(record.subject_uri)
                                        hostname = parsed_url.hostname

                                        data_processor.write_to_csv(hostname, soup.prettify())
                            except Exception as e:
                                continue
                            finally:
                                record = None
                                byte_lst = []
                    elif record:
                        byte_lst.append(line)
                    else:
                        continue


if __name__ == "__main__":
    main()

