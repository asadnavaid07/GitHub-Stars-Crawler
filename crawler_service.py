import os,time
from datetime import datetime
from typing import List,Dict,Optional,Iterator
from concurrent.futures import ThreadPoolExecutor,as_completed
from queue import Queue
from threading import Lock,Thread
import requests
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.pool import ThreadedConnectionPool

from github_client import GitHubClient
from models import RepositoryModel
from repository import RepositoryRepository


class CrawlerService:

    def __init__(self,api_client:'GitHubClient',repository:'RepositoryRepository',max_workers:int=10):
        self._api_client=api_client
        self._repository=repository
        self._max_workers=max_workers
        self._write_queue=Queue(maxsize=1000)
        self._total_crawled=0
        self._lock=Lock()

        

    def crawl(self,target_count:int=100_000,batch_size:int=100):

        start_time = time.time()
        writer_thread=Thread(target=self._background_writer,daemon=True)
        writer_thread.start()

        cursors=self._generate_cursors(target_count,batch_size)

        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures={
                executor.submit(self._fetch_batch,cursor,batch_size): cursor
                for cursor in cursors
            }

            for future in as_completed(futures):
                try:
                    batch_repos=future.result()

                    if batch_repos:
                        self._write_queue.put(batch_repos)
                        self._total_crawled+=len(batch_repos)

                        if self._total_crawled % 1000 == 0:
                                elapsed = time.time() - start_time
                                rate = self._total_crawled / elapsed if elapsed > 0 else 0
                                print(f"Progress: {self._total_crawled:,} repos | "
                                      f"Rate: {rate:.0f} repos/sec")
                        
                        if self._total_crawled >= target_count:
                            break
                            
                except Exception as e:
                    print(f"Batch error: {e}")

        self._write_queue.put(None)
        writer_thread.join()

        elapsed = time.time() - start_time
        rate = self._total_crawled / elapsed if elapsed > 0 else 0
        
        print(f"\n{'='*70}")
        print(f"Crawl completed!")
        print(f"Total repositories: {self._total_crawled:,}")
        print(f"Time elapsed: {elapsed:.2f}s ({elapsed/60:.1f} minutes)")
        print(f" Average speed: {rate:.0f} repos/second")
        print(f"{'='*70}")
        
        return self._total_crawled
    

    def generate_cusors(self,target_count:int,batch_size:int)->Iterator[Optional[str]]:

        cursor=None
        fetched=0

        while fetched< target_count:
            yield cursor
            fetched += batch_size

            if cursor is None:
                result = self._api_client.fetch_repositories(cursor, 1)
                if result.get('data', {}).get('search', {}).get('pageInfo', {}).get('endCursor'):
                    cursor = result['data']['search']['pageInfo']['endCursor']
    
    def _fetch_batch(self, cursor: Optional[str], 
                    batch_size: int) -> List[RepositoryModel]:

        try:
            result = self._api_client.fetch_repositories(cursor, batch_size)
            
            nodes = result.get('data', {}).get('search', {}).get('nodes', [])
 
            return [
                RepositoryModel.from_api_response(node)
                for node in nodes
                if node and 'databaseId' in node
            ]
            
        except Exception as e:
            print(f"Fetch error: {e}")
            return []
    
    def _background_writer(self):

        batch_buffer = []
        
        while True:
            batch = self._write_queue.get()
            
            if batch is None:  
                if batch_buffer:
                    self._repository.upsert_batch(batch_buffer)
                break
            
            batch_buffer.extend(batch)
            
            if len(batch_buffer) >= 500:
                self._repository.upsert_batch(batch_buffer)
                batch_buffer = []







