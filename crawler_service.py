import os,time
from datetime import datetime
from typing import List,Dict,Optional,Iterator,Tuple
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

        cursor = None
        pending_futures = set()
        
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            # Submit initial batch
            future = executor.submit(self._fetch_batch, cursor, batch_size)
            pending_futures.add(future)

            while pending_futures and self._total_crawled < target_count:
                # Wait for at least one future to complete
                done_future = next(as_completed(pending_futures), None)
                if not done_future:
                    break
                
                pending_futures.remove(done_future)
                
                try:
                    batch_repos, next_cursor = done_future.result()

                    if batch_repos:
                        self._write_queue.put(batch_repos)
                        self._total_crawled += len(batch_repos)

                        if self._total_crawled % 1000 == 0:
                            elapsed = time.time() - start_time
                            rate = self._total_crawled / elapsed if elapsed > 0 else 0
                            print(f"Progress: {self._total_crawled:,} repos | "
                                  f"Rate: {rate:.0f} repos/sec")
                    
                    # Submit next batch if we have a cursor and haven't reached target
                    if next_cursor and self._total_crawled < target_count:
                        new_future = executor.submit(self._fetch_batch, next_cursor, batch_size)
                        pending_futures.add(new_future)
                        
                except Exception as e:
                    print(f"Batch error: {e}")
                
                # Check if we should break
                if self._total_crawled >= target_count:
                    break

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
    
    def _fetch_batch(self, cursor: Optional[str], 
                    batch_size: int) -> Tuple[List[RepositoryModel], Optional[str]]:

        try:
            result = self._api_client.fetch_repositories(cursor, batch_size)
            
            if not result:
                return [], None
            
            data = result.get('data', {})
            search = data.get('search', {})
            nodes = search.get('nodes', [])
            page_info = search.get('pageInfo', {})
            next_cursor = page_info.get('endCursor') if page_info else None
 
            repos = [
                RepositoryModel.from_api_response(node)
                for node in nodes
                if node and 'databaseId' in node
            ]
            
            return repos, next_cursor
            
        except Exception as e:
            print(f"Fetch error: {e}")
            return [], None
    
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







