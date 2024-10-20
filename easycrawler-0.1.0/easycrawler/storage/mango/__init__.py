import threading

from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from easycrawler.core import Storage


class MongoDB(Storage):
    def __init__(self, uri: str, db_name: str):
        """
        初始化 MongoDB 客户端，连接到指定的数据库和集合。
        :param uri: MongoDB 连接 URI
        :param db_name: 数据库名称
        :param collection_name: 集合名称
        """
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self._lock = threading.Lock()

    def insert_one(self, table: str, document: dict):
        result = self.db[table].insert_one(document)
        return result.inserted_id

    def insert_many(self, table: str, documents: list):
        result = self.db[table].insert_many(documents)
        return result.inserted_ids

    def bulk_write(self, table: str, operations: list):
        try:
            result = self.db[table].bulk_write(operations)
            return result.bulk_api_result
        except BulkWriteError as bwe:
            print(f"批量写入错误: {bwe.details}")
            return None

    def update_one(self, table: str, filter_query: dict, update_values: dict):
        result = self.db[table].update_one(filter_query, {'$set': update_values})
        return result.modified_count

    def update_many(self, table: str, filter_query: dict, update_values: dict):
        result = self.db[table].update_many(filter_query, {'$set': update_values})
        return result.modified_count

    def delete_one(self, table: str, filter_query: dict):
        result = self.db[table].delete_one(filter_query)
        return result.deleted_count

    def delete_many(self, table: str, filter_query: dict):
        result = self.db[table].delete_many(filter_query)
        return result.deleted_count

    def find_one(self, table: str, filter_query: dict):
        return self.db[table].find_one(filter_query)

    def find_many(self, table: str, filter_query: dict, limit: int = 0):
        return self.db[table].find(filter_query).limit(limit)

