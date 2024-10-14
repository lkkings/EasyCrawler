# -*- coding: utf-8 -*-
"""
@Description: 
@Date       : 2024/10/14 20:52
@Author     : lkkings
@FileName:  : __init__.py.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :

"""
import logging
import sys
from colorama import Fore, Style, init

# 初始化 colorama (Windows 上需要)
init(autoreset=True)


class ColoredFormatter(logging.Formatter):
    # 设置不同日志级别的颜色
    COLORS = {
        logging.DEBUG: Fore.CYAN,
        logging.INFO: Fore.GREEN,
        logging.WARNING: Fore.YELLOW,
        logging.ERROR: Fore.RED,
        logging.CRITICAL: Fore.MAGENTA
    }

    def format(self, record):
        log_color = self.COLORS.get(record.levelno, Fore.WHITE)  # 获取颜色
        log_message = super().format(record)
        return f"{log_color}{log_message}{Style.RESET_ALL}"


class Logger:
    def __init__(self, name='my_logger', level=logging.DEBUG):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # 创建处理器并设置格式
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)

        formatter = ColoredFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                                     datefmt='%Y-%m-%d %H:%M:%S')
        console_handler.setFormatter(formatter)

        self.logger.addHandler(console_handler)

    def setLevel(self, level):
        self.logger.setLevel(level)

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)


logger = Logger()

