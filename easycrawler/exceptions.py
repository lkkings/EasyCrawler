class DLException(Exception):
    def __init__(self, *args):
        super().__init__(*args)


class NotSliceDLException(DLException):
    def __init__(self, *args):
        super().__init__(*args)


class TimeoutException(Exception):
    pass


class QueueEmptyException(Exception):
    pass


class RateLimitExceeded(Exception):
    """自定义异常，用于指示超过访问频率限制。"""
    pass


class ClientClosedException(Exception):
    pass


class NotUpdateException(Exception):
    pass
