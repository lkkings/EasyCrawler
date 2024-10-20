from abc import ABC, abstractmethod


class Storage(ABC):
    @abstractmethod
    def insert_one(self, table: str, document: dict):
        """插入单个文档"""
        pass

    @abstractmethod
    def insert_many(self,table:str,  documents: list):
        """批量插入多个文档"""
        pass

    @abstractmethod
    def bulk_write(self,table:str,  operations: list):
        """批量写入操作"""
        pass

    @abstractmethod
    def update_one(self,table:str,  filter_query: dict, update_values: dict):
        """更新单个文档"""
        pass

    @abstractmethod
    def update_many(self,table:str,  filter_query: dict, update_values: dict):
        """更新多个文档"""
        pass

    @abstractmethod
    def delete_one(self,table:str,  filter_query: dict):
        """删除单个文档"""
        pass

    @abstractmethod
    def delete_many(self,table:str,  filter_query: dict):
        """删除多个文档"""
        pass

    @abstractmethod
    def find_one(self,table:str,  filter_query: dict):
        """查找单个文档"""
        pass

    @abstractmethod
    def find_many(self,table:str,  filter_query: dict, limit: int = 0):
        """查找多个文档"""
        pass
