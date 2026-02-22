import os
import argparse

from main.utils.logger import setup_root_logger
from main.sources.confluence.confluence_document_reader import ConfluenceDocumentReader
from main.sources.confluence.confluence_document_converter import ConfluenceDocumentConverter
from main.sources.confluence.confluence_cloud_document_reader import ConfluenceCloudDocumentReader
from main.sources.confluence.confluence_cloud_document_converter import ConfluenceCloudDocumentConverter
from main.factories.create_collection_factory import create_collection_creator

setup_root_logger()

ap = argparse.ArgumentParser()
ap.add_argument("-collection", "--collection", required=True, help="Collection name (will be used as root folder name)")

ap.add_argument("-url", "--url", required=True, help="Confluence base url (e.g., https://your-domain.atlassian.net for Cloud or https://confluence.example.com for Server/Data Center)")
ap.add_argument("-cql", "--cql", required=False, default="", help="Confluence query (CQL) to get pages for indexing")

ap.add_argument("-indexers", "--indexers", required=False, default=["indexer_ChromaDb__embeddings_all-MiniLM-L6-v2"], help="List on indexer names", nargs='+')

ap.add_argument("-readOnlyFirstLevelComments", "--readOnlyFirstLevelComments", action="store_true", required=False, default=False, help="Confluence has hierarchical comments, first level comments are read by default, but for other ones additional call is needed what can slowdown the process. Pass this argument to read only first level comments and have better performance.")
args = vars(ap.parse_args())

# Detect if it's Confluence Cloud or Server/Data Center based on URL
is_cloud = args['url'].endswith('.atlassian.net')

if is_cloud:
    # Confluence Cloud setup
    email = os.environ.get('ATLASSIAN_EMAIL')
    api_token = os.environ.get('ATLASSIAN_TOKEN')

    if not email or not api_token:
        raise ValueError("Both 'ATLASSIAN_EMAIL' and 'ATLASSIAN_TOKEN' environment variables must be provided for Confluence Cloud.")

    confluence_document_reader = ConfluenceCloudDocumentReader(base_url=args['url'],
                                                               query=args['cql'],
                                                               email=email,
                                                               api_token=api_token,
                                                               read_all_comments=(not args['readOnlyFirstLevelComments']))
    confluence_document_converter = ConfluenceCloudDocumentConverter()

else:
    # Confluence Server/Data Center setup
    token = os.environ.get('CONF_TOKEN')
    login = os.environ.get('CONF_LOGIN')
    password = os.environ.get('CONF_PASSWORD')

    if not token and (not login or not password):
        raise ValueError("Either 'token' ('CONF_TOKEN' env variable) or both 'login' ('CONF_LOGIN' env variable) and 'password' ('CONF_PASSWORD' env variable) must be provided.")

    confluence_document_reader = ConfluenceDocumentReader(base_url=args['url'],
                                                          query=args['cql'],
                                                          token=token,
                                                          login=login, 
                                                          password=password,
                                                          read_all_comments=(not args['readOnlyFirstLevelComments']))
    confluence_document_converter = ConfluenceDocumentConverter()

confluence_collection_creator = create_collection_creator(collection_name=args['collection'],
                                                          indexers=args['indexers'],
                                                          document_reader=confluence_document_reader,
                                                          document_converter=confluence_document_converter)

confluence_collection_creator.run()