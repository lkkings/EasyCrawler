import threading

class ThreadSafeDict:
    def __init__(self):
        """初始化线程安全字典"""
        self._dict = {}
        self._lock = threading.RLock()  # 使用递归锁，以便在同一线程中允许多次锁定

    def __setitem__(self, key, value):
        """设置字典的键值对，支持 dict[key] = value 语法"""
        with self._lock:
            self._dict[key] = value

    def __getitem__(self, key):
        """通过键访问字典中的值，支持 dict[key] 语法"""
        with self._lock:
            return self._dict[key]

    def __delitem__(self, key):
        """删除字典中的键值对，支持 del dict[key] 语法"""
        with self._lock:
            del self._dict[key]

    def __contains__(self, key):
        """检查字典中是否存在指定的键，支持 'key in dict' 语法"""
        with self._lock:
            return key in self._dict

    def get(self, key, default=None):
        """获取字典中的值，如果键不存在则返回默认值"""
        with self._lock:
            return self._dict.get(key, default)

    def setdefault(self, key, default=None):
        """如果键不存在，则将其设置为默认值"""
        with self._lock:
            return self._dict.setdefault(key, default)

    def pop(self, key, default=None):
        """移除并返回字典中的键值对"""
        with self._lock:
            return self._dict.pop(key, default)

    def keys(self):
        """返回字典的所有键"""
        with self._lock:
            return list(self._dict.keys())

    def values(self):
        """返回字典的所有值"""
        with self._lock:
            return list(self._dict.values())

    def items(self):
        """返回字典的所有键值对"""
        with self._lock:
            return list(self._dict.items())

    def __len__(self):
        """返回字典中键值对的数量，支持 len(dict)"""
        with self._lock:
            return len(self._dict)

    def __repr__(self):
        """返回字典的字符串表示"""
        with self._lock:
            return repr(self._dict)

    def clear(self):
        """清空字典"""
        with self._lock:
            self._dict.clear()

    def update(self, other=None, **kwargs):
        """更新字典，支持传入另一个字典或关键字参数"""
        with self._lock:
            self._dict.update(other, **kwargs)
