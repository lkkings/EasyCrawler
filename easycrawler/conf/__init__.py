# -*- coding: utf-8 -*-
"""
@Description: 
@Date       : 2024/10/14 22:25
@Author     : lkkings
@FileName:  : __init__.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :

"""
import os.path
import sys
import typing
import yaml
from pathlib import Path


class ConfigManager:
    def __init__(self, filepath: str = 'conf.yaml'):
        self.filepath = Path(filepath)
        self.config = self.load_config()

    def load_config(self) -> dict:
        """从文件中加载配置 (Load the conf from the file)"""

        if not self.filepath.exists():
            raise FileExistsError("配置文件不存在", self.filepath)

        return yaml.safe_load(self.filepath.read_text(encoding="utf-8")) or {}

    def get_config(self, key: str, default=None) -> typing.Union[typing.Dict, str, int]:
        """
        从配置中获取给定键的值 (Get the value of the given key from the conf)

        Args:
            key: str: 获取键值
            default: any: 默认值 (default value)

        Return:
            self.config.get_config() 配置字典 (conf dict)
        """

        return self.config.get(key, default)


config: typing.Optional[ConfigManager] = None


def setup(config_path=None):
    global config
    if config_path is None:
        if len(sys.argv) > 0:
            _config_path = os.path.join(Path(os.path.abspath(sys.argv[0])).parent, 'conf.yaml')
        else:
            _config_path = os.path.join(os.getcwd(), 'conf.yaml')

        if not os.path.exists(_config_path):
            _config_path = Path('default.yaml')
    else:
        _config_path = config_path
    config = ConfigManager(filepath=_config_path)
