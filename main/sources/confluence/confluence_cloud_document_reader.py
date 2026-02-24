import requests
import urllib.parse
import logging

from ...utils.retry import execute_with_retry
from ...utils.batch import read_items_in_batches

class ConfluenceCloudDocumentReader:
    def __init__(self, 
                 base_url, 
                 query,
                 email=None,
                 api_token=None,
                 batch_size=50, 
                 number_of_retries=3, 
                 retry_delay=1, 
                 max_skipped_items_in_row=5,
                 read_all_comments=False,
                 timeout=60):
        # "email" and "api_token" must be provided for Cloud
        if not email or not api_token:
            raise ValueError("Both 'email' and 'api_token' must be provided for Confluence Cloud.")

        # Ensure base_url has the correct Cloud format
        if not base_url.endswith('.atlassian.net'):
            raise ValueError("Base URL must be a Confluence Cloud URL (ending with .atlassian.net)")
        
        self.base_url = base_url
        self.query = ConfluenceCloudDocumentReader.build_page_query(query)
        self.email = email
        self.api_token = api_token
        self.batch_size = batch_size
        # Confluence has hierarchical comments, we can read first level by adding "children.comment.body.storage" to "expand" parameter
        # but to read all comments we need to make additional request with "depth=all" parameter
        self.expand = "content.body.storage,content.ancestors,content.version,content.space,content.history,content.children.comment" if read_all_comments else "content.body.storage,content.ancestors,content.version,content.space,content.history,content.children.comment.body.storage"
        self.number_of_retries = number_of_retries
        self.retry_delay = retry_delay
        self.max_skipped_items_in_row = max_skipped_items_in_row
        self.read_all_comments = read_all_comments
        self.timeout = timeout
    
    def read_all_documents(self):
        for page in self.__read_items():
            yield {
                "page": page,
                "comments": self.__read_comments(page)
            }

    def get_number_of_documents(self):
        search_result = self.__request(
            self.__add_url_prefix('/wiki/rest/api/search'),
            {
                "cql": self.query,
                "limit": 1,
                "start": 0
            })
        
        return search_result['totalSize']
    
    def get_reader_details(self) -> dict:
        return {
            "type": "confluenceCloud",
            "baseUrl": self.base_url,
            "query": self.query,
            "expand": self.expand,
            "batchSize": self.batch_size,
            "readAllComments": self.read_all_comments,
        }
    
    @staticmethod
    def build_page_query(user_query):
        if not user_query:
            return "type=page"

        return f'type=page AND ({user_query})'

    def __add_url_prefix(self, relative_path):
        return self.base_url + relative_path
    
    def __read_comments(self, page):
        if page['content']['children']['comment']['size'] == 0:
            return []

        if not self.read_all_comments:
            return page['content']['children']['comment']['results']

        read_batch_func = lambda start_at, batch_size, cursor = None: self.__request(
            self.__add_url_prefix(f"/wiki/rest/api/content/{page['content']['id']}/child/comment"),
            {
                "limit": batch_size,
                "start": start_at,
                "expand": "body.storage",
                "depth": "all"
            })

        comments_generator = read_items_in_batches(read_batch_func,
                              fetch_items_from_result_func=lambda result: result['results'],
                              fetch_total_from_result_func=lambda result: result['size'],
                              batch_size=self.batch_size,
                              max_skipped_items_in_row=self.max_skipped_items_in_row,
                              itemsName="comments")

        try:
            return [comment for comment in comments_generator]
        except Exception as error:
            logging.warning(f"Failed to read comments for page {page['content']['id']}: {error}. Comments will be skipped.")
            return page['content']['children']['comment']['results']

    def __read_items(self):
        read_batch_func = lambda start_at, batch_size, cursor: self.__request(
            self.__add_url_prefix('/wiki/rest/api/search'),
            {
                "cql": self.query,
                "limit": batch_size,
                "start": start_at,
                "expand": self.expand,
                "cursor": cursor
            })

        return read_items_in_batches(read_batch_func,
                              fetch_items_from_result_func=lambda result: result['results'],
                              fetch_total_from_result_func=lambda result: result['totalSize'],
                              batch_size=self.batch_size,
                              max_skipped_items_in_row=self.max_skipped_items_in_row,
                              itemsName="pages",
                              cursor_parser=ConfluenceCloudDocumentReader.__parse_cursor)

    def __request(self, url, params):
        def do_request():
            response = requests.get(url=url, 
                                    headers={
                                        "Accept": "application/json",
                                        "Content-Type": "application/json"
                                    }, 
                                    params=params, 
                                    auth=(self.email, self.api_token),
                                    timeout=self.timeout)
            response.raise_for_status()

            return response.json()

        return execute_with_retry(do_request, f"Requesting items with params: {params}", self.number_of_retries, self.retry_delay)
    
    @staticmethod
    def __parse_cursor(result):
        if '_links' not in result:
            raise ValueError(f"No '_links' in the result: {result}")
        
        if 'next' not in result['_links']:
            return None

        next_link = result['_links']['next']
        url_params = ConfluenceCloudDocumentReader.parse_url_params(next_link)
        
        if 'cursor' not in url_params:
            raise ValueError(f"No 'cursor' parameter found in the next link: {next_link}")
        
        return url_params['cursor'][0]
    
    @staticmethod
    def parse_url_params(url):
        parsed = urllib.parse.urlparse(url)
        query_params = urllib.parse.parse_qs(parsed.query)
        return {key: values for key, values in query_params.items()} if query_params else {}