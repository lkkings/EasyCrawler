from typing import Dict

from easycrawler.core.base_storage import Storage
from easycrawler.storage.mango import MongoDB


def get_storage(cfg: Dict) -> Storage:
    storage_type = cfg.get('type')
    del cfg['type']
    storage_cfg = cfg.copy()
    if storage_type == 'mongodb':
        return MongoDB(**storage_cfg)
    else:
        raise TypeError(f'不支持的存储{storage_type}')
