import requests

from ...utils.retry import execute_with_retry
from ...utils.batch import read_items_in_batches

class JiraCloudDocumentReader:
    def __init__(self, 
                 base_url, 
                 query,
                 email=None,
                 api_token=None,
                 batch_size=500,
                 number_of_retries=3,
                 retry_delay=1,
                 max_skipped_items_in_row=5):
        # "email" and "api_token" must be provided for Cloud
        if not email or not api_token:
            raise ValueError("Both 'email' and 'api_token' must be provided for Jira Cloud.")

        # Ensure base_url has the correct Cloud format
        if not base_url.endswith('.atlassian.net'):
            raise ValueError("Base URL must be a Jira Cloud URL (ending with .atlassian.net)")
        
        self.base_url = base_url
        self.query = query
        self.email = email
        self.api_token = api_token
        self.batch_size = batch_size
        self.number_of_retries = number_of_retries
        self.retry_delay = retry_delay
        self.max_skipped_items_in_row = max_skipped_items_in_row
        self.fields = "summary,description,comment,updated,status,priority,labels,components,fixVersions,versions,assignee,reporter,issuetype,created"

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
            "type": "jiraCloud",
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
            response = requests.get(url=self.__add_url_prefix('/rest/api/3/search'), 
                                    headers={
                                        "Accept": "application/json",
                                        "Content-Type": "application/json"
                                    }, 
                                    params=params, 
                                    auth=(self.email, self.api_token))
            response.raise_for_status()

            return response.json()

        return execute_with_retry(do_request, f"Requesting items with params: {params}", self.number_of_retries, self.retry_delay) 