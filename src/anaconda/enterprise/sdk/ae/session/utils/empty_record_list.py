from ...constants import COLUMNS


class EmptyRecordList(list):
    def __init__(self, record_type, columns=None):
        self._record_type = record_type
        if columns is not None:
            self._columns = list(columns)
        else:
            self._columns = list(c for c in COLUMNS.get(record_type, ()) if not c.startswith("?"))
        super(EmptyRecordList, self).__init__()

    def __str__(self):
        return f"EmptyRecordList: record_type={self._record_type}\n  - columns: " + ",".join(self._columns)
