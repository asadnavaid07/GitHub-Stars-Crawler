import time
from typing import Dict,Optional
from threading import Lock
import requests



class GitHubClient:

    BASE_URL="https://api.github.com/graphql"

    def __init__(self,token:str):
        self._token=token
        self._headers={
            "Authorization":f"Bearer {token}",
            "Content-Type":"application/json"
        }
        self._rate_limit_remaining=5000
        self._rate_limit_reset_at=None
        self._lock=Lock()

    def fetch_repositories(self,cursor:Optional[str]=None,per_page:int=100)->Optional[Dict]:
        query="""
        query($cursor:String,$perPage:Int!){
            search(
            query: "stars:>1 sort:stars-desc"
            type: REPOSITORY
            first: $perPage
            after: $cursor){
            pageInfo{
            hasNextPage
            endCursor}
            nodes{
                ... on Repository {
                databaseId
                owner{
                login}
                name
                nameWithOwner
                stargazerCount
                createdAt
                updatedAt
                }
            }
         }
        }
        """
        variables = {
            "cursor": cursor,
            "perPage": per_page
        }
        try:
            result = self._execute_with_retry(query, variables)
            if result and 'data' in result and result.get('data'):
                return result
            elif result and 'errors' in result:
                error_messages = [err.get('message', 'Unknown error') for err in result.get('errors', [])]
                print(f"GraphQL API errors: {', '.join(error_messages)}")
                return None
            else:
                print(f"Unexpected API response format: {result}")
                return None
        except Exception as e:
            print(f"Error fetching repositories: {e}")
            import traceback
            traceback.print_exc()
            return None
    

    def _execute_with_retry(self,query:str,variables:Dict, max_retries:int=3)->Dict:

        for attempt in range(max_retries):
            try:
                with self._lock:
                    self._check_rate_limit()
                
                response= requests.post(
                    self.BASE_URL,
                    json={"query":query,"variables":variables},
                    headers=self._headers,
                    timeout=30
                )

                with self._lock:
                    self._update_rate_limit(response.headers)

                if response.status_code==200:
                    json_response = response.json()
                    # Check for GraphQL errors in response
                    if 'errors' in json_response:
                        error_messages = [err.get('message', 'Unknown error') for err in json_response.get('errors', [])]
                        raise Exception(f"GraphQL errors: {', '.join(error_messages)}")
                    return json_response
                if response.status_code==403:
                    self._wait_for_rate_limit_reset()
                    continue
                if response.status_code>=500:
                    wait=2**attempt
                    time.sleep(wait)
                    continue

                response.raise_for_status()

            except requests.exceptions.RequestException as e:
                if attempt ==max_retries-1:
                    raise
                time.sleep(2**attempt)
        
        raise Exception("Max retries exceeded")
    
    def _check_rate_limit(self):
        if self._rate_limit_remaining < 50:
            self._wait_for_rate_limit_reset()

    def _update_rate_limit(self,headers:Dict):
        if "X-RateLimit-Remaining" in headers:
            self._rate_limit_remaining=int(headers["X-RateLimit-Remaining"])

        if "X-RateLimit-Reset" in headers:
            self._rate_limit_reset_at = int(headers["X-RateLimit-Reset"])

    def _wait_for_rate_limit_reset(self):
        if self._rate_limit_reset_at:
            wait_time = max(0, self._rate_limit_reset_at - time.time())
            if wait_time > 0:
                print(f"Rate limit reached, waiting {wait_time:.0f}s...")
                time.sleep(wait_time + 1)


