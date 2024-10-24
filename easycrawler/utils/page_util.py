import platform
import threading
import typing

from DrissionPage import ChromiumOptions, ChromiumPage
from easycrawler.utils.common import rate_limit


class Pager:
    def __init__(self, co: ChromiumOptions):
        self.co: ChromiumOptions = co
        self.page: typing.Optional[ChromiumPage] = None

    @classmethod
    def get_base_co(cls):
        if platform.system().lower() == 'windows':
            co = ChromiumOptions()
        else:
            co = ChromiumOptions()
            co.headless(True)
            co.set_argument(
                '--no-sandbox')
            co.set_argument("--disable-gpu")
        co = co.no_imgs(True).no_js(True).auto_port()
        return co

    def __enter__(self):
        self.page = ChromiumPage(self.co)
        return self.page

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.page.quit(1, True)
        except:
            pass
        return True
