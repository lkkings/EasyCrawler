# -*- coding: utf-8 -*-
"""
@Description: 
@Date       : 2024/10/14 23:38
@Author     : lkkings
@FileName:  : redis_util.py
@Github     : https://github.com/lkkings
@Mail       : lkkings888@gmail.com
-------------------------------------------------
Change Log  :

"""
import hashlib
import math
from typing import List, Dict, Any

import redis


class NamedSet:
    def __init__(self, redis_conn: redis.Redis, name: str):
        self.redis_conn: redis.Redis = redis_conn
        self.name = name

    def add(self, item):
        self.redis_conn.sadd(self.name, item)

    def all(self):
        return self.redis_conn.smembers(self.name)

    def batch_insert(self, data):
        with self.redis_conn.pipeline() as pipe:
            for item in data:
                pipe.sadd(self.name, item)
            pipe.execute()

    def clear(self):
        self.redis_conn.delete(self.name)

    def pop(self):
        return self.redis_conn.spop(self.name)

    def size(self):
        return self.redis_conn.scard(self.name)

    def exists(self, value):
        return self.redis_conn.sismember(self.name, value)

    def is_empty(self):
        return self.size() == 0


class NamedList:
    def __init__(self, redis_conn: redis.Redis, name: str):
        self.redis_conn: redis.Redis = redis_conn
        self.name = name

    def clear(self):
        self.redis_conn.delete(self.name)

    def append(self, item):
        self.redis_conn.rpush(self.name, item)

    def prepend(self, item):
        self.redis_conn.lpush(self.name, item)

    def get(self, index):
        return self.redis_conn.lindex(self.name, index)

    def remove(self, item):
        self.redis_conn.lrem(self.name, 0, item)

    def size(self):
        return self.redis_conn.llen(self.name)

    def pop(self):
        return self.redis_conn.lpop(self.name)

    def is_empty(self):
        return self.size() == 0

    def all(self):
        return self.redis_conn.lrange(self.name, 0, -1)

    def batch_insert(self, data: List[Any]):
        with self.redis_conn.pipeline() as pipe:
            for item in data:
                pipe.rpush(self.name, item)
            pipe.execute()


class BloomFilter:
    def __init__(self, redis_conn, name, capacity, error_rate=0.05):
        self.redis_conn = redis_conn
        self.name = name
        self.capacity = capacity
        self.error_rate = error_rate

        # Calculate optimal number of bits and hash functions
        self.num_bits = math.ceil(capacity * abs(math.log(error_rate)) / (math.log(2) ** 2))
        self.num_hashes = math.ceil(self.num_bits * math.log(2) / capacity)

        # Redis key names
        self.bit_array_key = f"{self.name}:bitarray"
        self.num_hashes_key = f"{self.name}:num_hashes"

    def add(self, item):
        hashes = self._calculate_hashes(item)
        for hash_value in hashes:
            self.redis_conn.setbit(self.bit_array_key, hash_value % self.num_bits, 1)

    def exists(self, item):
        hashes = self._calculate_hashes(item)
        for hash_value in hashes:
            if not self.redis_conn.getbit(self.bit_array_key, hash_value % self.num_bits):
                return False
        return True

    def _calculate_hashes(self, item):
        hashes = []
        md5 = hashlib.md5()
        md5.update(item.encode('utf-8'))
        seed = int(md5.hexdigest(), 16)

        for i in range(self.num_hashes):
            hash_value = (seed + i) % self.num_bits
            hashes.append(hash_value)
        return hashes


class NamedHashMap:
    def __init__(self, redis_conn, name):
        self.redis_conn = redis_conn
        self.name = name

    def set(self, key, value):
        self.redis_conn.hset(self.name, key, value)

    def get(self, key):
        return self.redis_conn.hget(self.name, key)

    def delete(self, key):
        self.redis_conn.hdel(self.name, key)

    def exists(self, key):
        return self.redis_conn.hexists(self.name, key)

    def size(self):
        return self.redis_conn.hlen(self.name)

    def keys(self):
        return self.redis_conn.hkeys(self.name)

    def values(self):
        return self.redis_conn.hvals(self.name)

    def items(self):
        return self.redis_conn.hgetall(self.name)

    def clear(self):
        self.redis_conn.delete(self.name)

    def batch_insert(self, items: Dict[str, str]):
        with self.redis_conn.pipeline() as pipe:
            for key, value in items.items():
                pipe.hset(self.name, key, value)
            pipe.execute()
