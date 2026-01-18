import logging
import requests

from ...utils.retry import execute_with_retry

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
        search_result = self.__request_items(None)
        total_count = 0
        for _ in search_result['issues']:
            total_count += 1
        
        while not search_result.get('isLast', True):
            search_result = self.__request_items(search_result.get('nextPageToken'))
            total_count += len(search_result['issues'])
        
        return total_count

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
        has_more_items = True
        next_page_token = None

        while has_more_items:
            read_result = self.__request_items(next_page_token)

            issues = read_result.get('issues', [])
            
            logging.debug(f"New batch with {len(issues)} items was read")

            for issue in issues:
                yield issue

            next_page_token = read_result.get('nextPageToken')
            has_more_items = not read_result.get('isLast', True)

    def __request_items(self, next_page_token=None):
        def do_request():
            params = {
                'jql': self.query,
                'fields': self.fields,
            }
            
            if next_page_token:
                params['nextPageToken'] = next_page_token
            
            response = requests.get(
                url=self.__add_url_prefix('/rest/api/3/search/jql'), 
                headers={
                    "Accept": "application/json"
                }, 
                params=params, 
                auth=(self.email, self.api_token)
            )
            response.raise_for_status()

            return response.json()

        return execute_with_retry(do_request, f"Requesting items with nextPageToken: {next_page_token}", self.number_of_retries, self.retry_delay) 