import os
from crawler_service import CrawlerService
from github_client import GitHubClient
from repository import RepositoryRepository


def main():
    
    github_token = os.environ.get('GITHUB_TOKEN')
    if not github_token:
        raise ValueError("GITHUB_TOKEN environment variable is required")
    
    db_url = os.environ.get('DATABASE_URL')
    if not db_url:
        raise ValueError("DATABASE_URL environment variable is required")
  

    api_client = GitHubClient(github_token)
    repository = RepositoryRepository(db_url, min_conn=10, max_conn=20)
 

    crawler = CrawlerService(
        api_client=api_client,
        repository=repository,
        max_workers=10  
    )
    
    try:
        target_count = int(os.environ.get('TARGET_COUNT', '100000'))
    
        crawler.crawl(target_count=target_count)
        # crawler.crawl(target_count=100_000, batch_size=100)
        
    finally:
        repository.close()


if __name__ == "__main__":
    main()