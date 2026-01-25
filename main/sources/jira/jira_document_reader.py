import requests

from ...utils.retry import execute_with_retry
from ...utils.batch import read_items_in_batches

class JiraDocumentReader:
    def __init__(self, 
                 base_url, 
                 query,
                 token=None,
                 login=None, 
                 password=None,
                 batch_size=500,
                 number_of_retries=3,
                 retry_delay=1,
                 max_skipped_items_in_row=5,
                 timeout=60):
        # "token" or "login" and "password" must be provided
        if not token and (not login or not password):
            raise ValueError("Either 'token' or both 'login' and 'password' must be provided.")

        self.base_url = base_url
        self.query = query
        self.token = token
        self.login = login
        self.password = password
        self.batch_size = batch_size
        self.number_of_retries = number_of_retries
        self.retry_delay = retry_delay
        self.max_skipped_items_in_row = max_skipped_items_in_row
        self.timeout = timeout
        self.fields = "summary,description,comment,created,updated,epic,parent,status,priority,assignee,reporter,issuetype"

    def read_all_documents(self):
        return self.__read_items()

    def get_number_of_documents(self):
        search_result = self.__request_items({
            'jql': self.query, 
            "startAt": 0, 
            "maxResults": 1
        })

        return search_result['total']

    def get_reader_details(self) -> dict:
        return {
            "type": "jira",
            "baseUrl": self.base_url,
            "query": self.query,
            "batchSize": self.batch_size,
            "fields": self.fields,
        }

    def __add_url_prefix(self, relative_path):
        return self.base_url + relative_path

    def __read_items(self):
        read_batch_func = lambda start_at, batch_size: self.__request_items({
            'jql': self.query, 
            "startAt": start_at, 
            "maxResults": batch_size,
            "fields": self.fields,
        })

        return read_items_in_batches(read_batch_func,
                              fetch_items_from_result_func=lambda result: result['issues'],
                              fetch_total_from_result_func=lambda result: result['total'],
                              batch_size=self.batch_size,
                              max_skipped_items_in_row=self.max_skipped_items_in_row)



    def __request_items(self, params):
        def do_request():
            response = requests.get(url=self.__add_url_prefix('/rest/api/latest/search'), 
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