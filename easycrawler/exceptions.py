class DLException(Exception):
    def __init__(self, *args):
        super().__init__(*args)


class NotSliceDLException(DLException):
    def __init__(self, *args):
        super().__init__(*args)
