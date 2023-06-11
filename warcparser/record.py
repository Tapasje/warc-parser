class WARCRecord:
    def __init__(self, warc_id: str, data_length: str, record_type: str, subject_uri: str, 
                 creation_date: str, content_type: str, record_id: str) -> None:
        self.warc_id = warc_id
        self.data_length = int(data_length)
        self.record_type = record_type
        self.subject_uri = subject_uri
        self.creation_data = creation_date
        self.content_type = content_type
        self.record_id = record_id

    def print(self) -> None:
        print("Record: {{ warc-id: {}, data-length: {}, record-type: {}, subject-uri: {} }}"
                     .format(self.warc_id, self.data_length, self.record_type, self.subject_uri))