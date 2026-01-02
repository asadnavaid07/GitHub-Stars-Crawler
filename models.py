import os,time
from datetime import datetime
from typing import List,Dict,Optional,Iterator
from concurrent.futures import ThreadPoolExecutor,as_completed
from queue import Queue
from threading import Lock
import requests
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.pool import ThreadedConnectionPool


class RepositoryModel:

    __slots__= ('_db_id', '_owner', '_name', '_full_name', '_star_count', 
                 '_created_at', '_updated_at')
    
    def __init__(self,db_id:int, owner:str,name:str, full_name:str,star_count:int,created_at:str,updated_at:str):
        object.__setattr__(self,'_db_id',db_id)
        object.__setattr__(self, '_owner', owner)
        object.__setattr__(self, '_name', name)
        object.__setattr__(self, '_full_name', full_name)
        object.__setattr__(self, '_star_count', star_count)
        object.__setattr__(self, '_created_at', created_at)
        object.__setattr__(self, '_updated_at', updated_at)


    def __setattr__(self, name, value):
        raise AttributeError(f"Cannot modify attribute '{name}' of immutable instance")
    
    @property
    def db_id(self)->int:
        return self._db_id
    
    @property
    def owner(self)->str:
        return self._owner
    
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def full_name(self) -> str:
        return self._full_name
    
    @property
    def star_count(self) -> int:
        return self._star_count
    
    @property
    def created_at(self) -> str:
        return self._created_at
    
    @property
    def updated_at(self) -> str:
        return self._updated_at
    
    @classmethod
    def from_api_response(cls,node:Dict)->'RepositoryModel':
        return cls(
            db_id=node['databaseId'],
            owner=node['owner']['login'],
            name=node['name'],
            full_name=node['nameWithOwner'],
            star_count=node['stargazerCount'],
            created_at=node['createdAt'],
            updated_at=node['updatedAt']
        )
