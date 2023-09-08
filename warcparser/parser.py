from record import WARCRecord
import gzip
import os
import logging
from tqdm import tqdm
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import csv
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

    if file.startswith(('/index.', '/default.', '/home.', '/main.', '/welcome.')):
        return path, domain

    return None, None


if __name__ == "__main__":

    data_processor = DataProcessor('../data/raw/webspam-uk2007-set1-1.0/WEBSPAM-UK2007-SET1-labels.txt',
                                   '../data/raw/webspam-uk2007-set1-1.0/WEBSPAM-UK2007-hostnames.txt',
                                   '../data/processed/train_dataset.csv')

    for i in range(8):
        file_path = f"../data/raw/law{i}.warc.gz"
        file_size = os.stat(file_path).st_size

        print(f"Now processing: {file_path}")
        with tqdm(total=file_size, unit_scale=True, unit_divisor=1024, unit="B") as pbar:
            with gzip.open(file_path, 'r') as warc_file:

                record = None
                byte_list = []

                for line in warc_file:
                    pbar.update(len(line))
                    try:
                        if line.startswith(b'warc/0.9'):
                            if record is not None:
                                try:
                                    path, domain = get_homepage(
                                        record.subject_uri)
                                    if path is not None and domain is not None:
                                        html_content = extract_html(byte_list)
                                        soup = BeautifulSoup(
                                            html_content, features='html.parser')

                                        # Check if the page is in English
                                        text = ''.join(soup.stripped_strings)
                                        if detect(text) == 'en':
                                            parsed_url = urlparse(
                                                record.subject_uri)
                                            hostname = parsed_url.hostname

                                            data_processor.write_to_csv(
                                                hostname, html_content)
                                except Exception as e:
                                    continue
                                finally:
                                    record = None
                                    byte_list = []
                            else:
                                # Create a new record
                                record = WARCRecord(*(str(line).split()))
                                # Check if the hostname is in the dictionary, else set record to None
                                parsed_url = urlparse(
                                    record.subject_uri)
                                if not data_processor.is_hostname_in_dict(parsed_url.hostname):
                                    record = None
                        elif record is not None:
                            byte_list.append(line)
                        else:
                            continue
                    except Exception as e:
                        logging.error(e)

                # TODO: handle the last record
