import csv
import htmlmin

from pathlib import Path


class DataProcessor:
    def __init__(self, labels_file, hostnames_file, output_file, flush_interval=1000):
        p = Path(output_file).resolve()

        # Step 1: Create dictionaries to store data
        self.data_dict = {}

        # Initialize the CSV file and keep it open
        self.html_file = open(str(p) + '/html_data.csv', 'w', newline='', encoding='utf-8')

        self.html_csv_writer = csv.writer(self.html_file, delimiter='\t', quoting=csv.QUOTE_NONNUMERIC)

        self.html_csv_writer.writerow(['html', 'label'])

        # Step 2: Read the file with labels
        with open(labels_file, 'r') as labels_file:
            for line in labels_file:
                parts = line.split()
                if len(parts) >= 2:
                    hostid = int(parts[0])
                    label = str(parts[1])
                    if (label != 'undecided'):
                        self.data_dict[hostid] = {'label': label}
                    elif parts[2] != '-':
                        spam_probability = float(parts[2])
                        if spam_probability >= 0.5:
                            self.data_dict[hostid] = {'label': 'spam'}

        # Step 3: Read the file with hostname-to-hostname ID mapping
        with open(hostnames_file, 'r') as hostnames_file:
            for line in hostnames_file:
                parts = line.split()
                if len(parts) >= 2:
                    hostid = int(parts[0])
                    hostname = str(parts[1])
                    if hostid in self.data_dict:
                        self.data_dict[hostid]['hostname'] = hostname

        # Flip the dictionary structure
        self.flipped_data_dict = {}
        for hostid, data in self.data_dict.items():
            self.flipped_data_dict[data['hostname']] = data['label']

        del self.data_dict  # Delete the old dictionary

    def write_to_csv(self, hostname, html):
        # Append data to the existing CSV file
        if (hostname in self.flipped_data_dict):
            minified = htmlmin.minify(html, remove_empty_space=True)
            
            cleaned_html = minified.replace('\n', ' ').replace('\r', '')

            label = self.flipped_data_dict[hostname]

            self.html_csv_writer.writerow([cleaned_html, label])
            
    def is_hostname_in_dict(self, hostname):
        """Check if a hostname is present in the flipped_data_dict."""
        return hostname in self.flipped_data_dict

    def __del__(self):
        if hasattr(self, 'html_data'):
            self.html_data.close()
