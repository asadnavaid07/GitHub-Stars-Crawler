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

from models import RepositoryModel


class RepositoryRepository:

    def __init__(self,db_url:str,min_conn:int=5,max_conn:int=20):
        self._pool=ThreadedConnectionPool(min_conn,max_conn,db_url)

    def upsert_batch(self,repositories:List['RepositoryModel']):

        if not repositories:
            return 0
        
        conn=self._pool.getconn()

        try:
            curr=conn.cursor()

            repo_data=[
                 (r.db_id, r.owner, r.name, r.full_name, r.created_at, r.updated_at)
                 for r in repositories
            ]
        
            execute_batch(curr,"""
            INSERT INTO repositories(id, owner, name, full_name, created_at, updated_at, last_crawled_at)
            VALUES (%s,%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (id)
            DO UPDATE SET
                          updated_at = EXCLUDED.updated_at,
                          last_crawled_at=NOW()
            """,repo_data,page_size=1000
            )
            star_data = [(r.db_id, r.star_count) for r in repositories]
            execute_batch(curr, """
                INSERT INTO repository_stars (repository_id, star_count, observed_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (repository_id, observed_at) 
                DO UPDATE SET star_count = EXCLUDED.star_count
            """, star_data, page_size=1000)

            conn.commit()

            return len(repositories)
        

        except Exception as e:
            conn.rollback()
            raise e
        
        finally:
            curr.close()
            self._pool.putconn(conn)

    
    def get_count(self)->int:
        conn=self._pool.getconn()

        try:
            curr=conn.cursor()
            curr.execute("SELECT COUNT(*) FROM repositories")

            return curr.fetchone()[0]
        
        finally:
            curr.close()
            self._pool.putconn(conn)

    def close(self):
        self._pool.closeall()
