import os
import argparse

from main.utils.logger import setup_root_logger
from main.sources.jira.jira_document_reader import JiraDocumentReader
from main.sources.jira.jira_document_converter import JiraDocumentConverter
from main.sources.jira.jira_cloud_document_reader import JiraCloudDocumentReader
from main.sources.jira.jira_cloud_document_converter import JiraCloudDocumentConverter
from main.factories.create_collection_factory import create_collection_creator
from main.splitter.text_splitter import TextSplitter

setup_root_logger()

ap = argparse.ArgumentParser()
ap.add_argument("-collection", "--collection", required=True, help="Collection name (will be used as root folder name)")

ap.add_argument("-url", "--url", required=True, help="Jira base url (Cloud: https://your-domain.atlassian.net, Server/Data Center: https://jira.example.com)")
ap.add_argument("-jql", "--jql", required=False, default="",
                
                 help="Jira query (JQL) to get tickets for indexing")

ap.add_argument("-indexers", "--indexers", required=False, default=["indexer_ChromaDb__embeddings_all-MiniLM-L6-v2", "indexer_SqlLiteBM25"], help="list on indexer names", nargs='+')
ap.add_argument("-chunkSize", "--chunkSize", required=False, default=1000, type=int, help="Chunk size for text splitting (default: 1000)")
ap.add_argument("-chunkOverlap", "--chunkOverlap", required=False, default=100, type=int, help="Chunk overlap for text splitting (default: 100)")
args = vars(ap.parse_args())

text_splitter = TextSplitter(chunk_size=args['chunkSize'], chunk_overlap=args['chunkOverlap'])

# Detect if it's Jira Cloud or Server/Data Center based on URL
is_cloud = args['url'].endswith('.atlassian.net')

if is_cloud:
    # Jira Cloud authentication
    email = os.environ.get('ATLASSIAN_EMAIL')
    api_token = os.environ.get('ATLASSIAN_TOKEN')
    
    if not email or not api_token:
        raise ValueError("Both 'ATLASSIAN_EMAIL' and 'ATLASSIAN_TOKEN' environment variables must be provided for Jira Cloud.")
    
    jira_document_reader = JiraCloudDocumentReader(base_url=args['url'],
                                                   query=args['jql'],
                                                   email=email,
                                                   api_token=api_token)
    
    jira_document_converter = JiraCloudDocumentConverter(text_splitter)
    
else:
    # Jira Server/Data Center authentication
    token = os.environ.get('JIRA_TOKEN')
    login = os.environ.get('JIRA_LOGIN')
    password = os.environ.get('JIRA_PASSWORD')

    if not token and (not login or not password):
        raise ValueError("Either 'token' ('JIRA_TOKEN' env variable) or both 'login' ('JIRA_LOGIN' env variable) and 'password' ('JIRA_PASSWORD' env variable) must be provided for Jira Server/Data Center.")

    jira_document_reader = JiraDocumentReader(base_url=args['url'],
                                              query=args['jql'],
                                              token=token,
                                              login=login, 
                                              password=password)
    
    jira_document_converter = JiraDocumentConverter(text_splitter)

jira_collection_creator = create_collection_creator(collection_name=args['collection'],
                                                     indexers=args['indexers'],
                                                     document_reader=jira_document_reader,
                                                     document_converter=jira_document_converter)

jira_collection_creator.run()