# -*- coding: utf-8 -*-
"""
@Description:
@Date       : 2024/10/14 23:03
@Author     : lkkings
@FileName:  : _http.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :

"""
import time
import typing

from curl_cffi import requests


class Request:

    def __init__(
            self,
            url: str,
            method: str = "GET",
            headers: typing.Dict = None,
            sleep: float = 0.5,
            retries: int = 3,
            timeout: float = 10.0,
            **kwargs: typing.Any
    ) -> None:
        self.method = str(method).upper()
        self.url = url
        self._sleep = sleep
        self._retries = retries
        self._delay = 0
        self.timeout = timeout
        self.kwargs = kwargs
        self.headers = headers if headers else {}

    def __repr__(self):
        return f'Request({self.url})'

    @property
    def delay(self):
        return self._delay

    def send(self) -> str:
        try:
            self.kwargs['verify'] = self.kwargs.get('verify', True)
            headers = {'Accept-Encoding': 'gzip, deflate, br'}
            self.headers.update(headers)
            if self.url.startswith('http://127.0.0.1'):
                self.kwargs['proxy'] = None
            start_time = time.time()
            resp = requests.request(method=self.method,
                                    url=self.url,
                                    timeout=self.timeout,
                                    headers=self.headers,
                                    **self.kwargs)
            self._delay = time.time() - start_time
            return resp.text
        except Exception as e:
            if self._retries > 0:
                self._retries -= 1
                time.sleep(self._sleep)
                return self.send()
            raise e

