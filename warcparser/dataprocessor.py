import csv
import htmlmin


class DataProcessor:
    def __init__(self, labels_file, hostnames_file, output_file, flush_interval=1000):
        # Step 1: Create dictionaries to store data
        self.data_dict = {}
        self.output_file = output_file
        self.flush_interval = flush_interval
        self.operations_since_last_flush = 0

        # Initialize the CSV file and keep it open
        self.csvfile = open(self.output_file, 'w', newline='')
        self.csv_writer = csv.writer(self.csvfile)
        self.csv_writer.writerow(['text', 'label'])

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
            html = f'"{htmlmin.minify(html, remove_empty_space=True)}"'

            self.csv_writer.writerow(
                [html, self.flipped_data_dict[hostname]])

            # Increment the operation count since the last flush
            self.operations_since_last_flush += 1

            # Flush and close the file if the flush interval is reached
            if self.operations_since_last_flush >= self.flush_interval:
                self.flush_and_close()

    def is_hostname_in_dict(self, hostname):
        """Check if a hostname is present in the flipped_data_dict."""
        return hostname in self.flipped_data_dict

    def __del__(self):
        # Close the CSV file when the DpipataProcessor instance is deleted
        if hasattr(self, 'csvfile'):
            self.csvfile.close()
