import requests
import logging

from ...utils.retry import execute_with_retry
from ...utils.batch import read_items_in_batches

class ConfluenceDocumentReader:
    def __init__(self, 
                 base_url, 
                 query,
                 token=None,
                 login=None, 
                 password=None,
                 batch_size=50, 
                 number_of_retries=3, 
                 retry_delay=1, 
                 max_skipped_items_in_row=5,
                 read_all_comments=False,
                 timeout=60):
        # "token" or "login" and "password" must be provided
        if not token and (not login or not password):
            raise ValueError("Either 'token' or both 'login' and 'password' must be provided.")

        self.base_url = base_url
        self.query = ConfluenceDocumentReader.build_page_query(query)
        self.token = token
        self.login = login
        self.password = password
        self.batch_size = batch_size
        # Confluence has hierarchical comments, we can read first level by adding "children.comment.body.storage" to "expand" parameter
        # but to read all comments we need to make additional request with "depth=all" parameter
        self.expand = "body.storage,ancestors,version,space,history,children.comment" if read_all_comments else "body.storage,ancestors,version,space,history,children.comment.body.storage"
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
            self.__add_url_prefix('/rest/api/content/search'),
            {
                "cql": self.query,
                "limit": 1,
                "start": 0
            })

        return search_result['totalSize']
    
    def get_reader_details(self) -> dict:
        return {
            "type": "confluence",
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
        if page['children']['comment']['size'] == 0:
            return []

        if not self.read_all_comments:
            return page['children']['comment']['results']

        read_batch_func = lambda start_at, batch_size: self.__request(
            self.__add_url_prefix(f"/rest/api/content/{page['id']}/child/comment"),
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
            logging.warning(f"Failed to read comments for page {page['id']}: {error}. Comments will be skipped.")
            return page['children']['comment']['results']

    def __read_items(self):
        read_batch_func = lambda start_at, batch_size: self.__request(
            self.__add_url_prefix('/rest/api/content/search'),
            {
                "cql": self.query,
                "limit": batch_size,
                "start": start_at,
                "expand": self.expand
            })

        return read_items_in_batches(read_batch_func,
                              fetch_items_from_result_func=lambda result: result['results'],
                              fetch_total_from_result_func=lambda result: result['totalSize'],
                              batch_size=self.batch_size,
                              max_skipped_items_in_row=self.max_skipped_items_in_row,
                              itemsName="pages")

    def __request(self, url, params):
        def do_request():
            response = requests.get(url=url, 
                                    headers={
                                        "Accept": "application/json",
                                        **({"Authorization": f"Bearer {self.token}"} if self.token else {})
                                    }, 
                                    params=params, 
                                    auth=((self.login, self.password) if self.login and self.password else None),
                                    timeout=self.timeout)
            response.raise_for_status()
            return response.json()

        return execute_with_retry(do_request, f"Requesting items with params: {params}", self.number_of_retries, self.retry_delay)

