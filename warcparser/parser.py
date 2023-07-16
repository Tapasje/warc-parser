from record import WARCRecord
import gzip
import os
import re
import logging
from tqdm import tqdm
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from langdetect import detect_langs

def sanitize_dirname(dirname):
    # Remove invalid characters
    dirname = re.sub(r'[<>:"/\\|?*]', '', dirname)

    # Remove leading/trailing whitespace and dots
    dirname = dirname.strip(' .')

    # Replace reserved names
    reserved_names = ['CON', 'PRN', 'AUX', 'NUL', 'COM1', 'COM2', 'COM3', 'COM4', 
                      'COM5', 'COM6', 'COM7', 'COM8', 'COM9', 'LPT1', 'LPT2', 
                      'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9']
    if dirname.upper() in reserved_names:
        dirname = '_' + dirname

    return dirname

def sanitize_path(path):
    # Split the path into directory names
    parts = path.split('/')

    # Sanitize each directory name
    parts = [sanitize_dirname(part) for part in parts]

    # Rejoin the sanitized directory names into a path
    sanitized_path = '/'.join(parts)

    return sanitized_path

def sanitize_filename(filename, replacement='_'):
    # Illegal characters for file names in Unix-based systems
    invalid_unix = {'/', '\0'}

    # In Windows, some characters might be valid in a path, but not in a file name
    invalid_windows = {'<', '>', ':', '"', '/', '\\', '|', '?', '*'}

    # Merge invalid character sets
    invalid = invalid_unix.union(invalid_windows)

    # Create translation table from invalid characters to the replacement character
    trans_table = str.maketrans({char: replacement for char in invalid})

    # Clean the filename
    clean_name = filename.translate(trans_table)

    # In addition to the above, the names "." and ".." are not valid names in Windows, so handle those separately
    if clean_name in {".", ".."}:
        clean_name = replacement

    return clean_name

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
        html_content = b''.join(byte_list[start_index:end_index]).decode('utf-8', errors="ignore")
    else:
        html_content = ""

    return html_content

def is_english(soup):
    try:
        # Extract all the text from the page
        text = ' '.join(soup.stripped_strings)

        # Detect the languages
        detections = detect_langs(text)

        for detection in detections:
            if detection.lang == 'en' and detection.prob > 0.6:
                return True
        return False
    except:
        return False

def get_homepage(subject_uri: str):
    parsed_url = urlparse(subject_uri)
    domain = parsed_url.netloc.lower()
    path = parsed_url.path.lower()

    if path == '/' or not path:
        return '/index.html', domain

    if path.startswith(('/index.', '/default.', '/home.')):
        return path, domain
    
    return None, None

if __name__ == "__main__":
    for i in range(8):
        file_path = f"../data/law{i}.warc.gz"
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
                                    path, domain = get_homepage(record.subject_uri)
                                    if path is not None and domain is not None:                             
                                        path = os.path.normpath(path)
                                        path, filename = os.path.split(path)
                                        path, filename = (sanitize_path(path), sanitize_filename(filename))

                                        basedir = os.path.join('html', domain, path.strip("\\"))

                                        if not os.path.exists(os.path.join(basedir, filename)):
                                            if (path == ''):
                                                html = extract_html(byte_list)
                                                soup = BeautifulSoup(html, features='html.parser')

                                                if is_english(soup):
                                                    if not os.path.isdir(basedir):
                                                        os.makedirs(basedir, exist_ok=True)

                                                    with open(os.path.join(basedir, filename), 'w', encoding='utf-8') as f:            
                                                        f.write(soup.prettify())
                                                        logging.info('Finished parsing %s' % record.subject_uri)

                                except Exception as e:
                                    raise Exception(e)
                                    # logging.info(e)
                                finally:
                                    record = None
                                    byte_list = []
                            else: 
                                record = WARCRecord(*(str(line).split()))
                        elif record is not None:
                            byte_list.append(line)
                    except Exception as e:
                        logging.error(e)
                
                # Do something with last record
