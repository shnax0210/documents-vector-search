import os
import argparse

from main.sources.jira.jira_document_reader import JiraDocumentReader
from main.sources.jira.jira_document_converter import JiraDocumentConverter
from main.factories.create_collection_factory import create_collection_creator

ap = argparse.ArgumentParser()
ap.add_argument("-url", "--url", required=True, help="jira base url")
ap.add_argument("-jql", "--jql", required=True, help="jql query to get tickets for label adding")
ap.add_argument("-collection", "--collection", required=True, help="collaction name (will be used as root folder name)")
ap.add_argument("-indexers", "--indexers", required=False, default=["indexer_FAISS_IndexFlatL2__embeddings_all-MiniLM-L6-v2"], help="list on indexer names", nargs='+')
args = vars(ap.parse_args())

token = os.environ.get('JIRA_TOKEN')
login=os.environ.get('JIRA_LOGIN')
password=os.environ.get('JIRA_PASSWORD')

if not token and (not login or not password):
    raise ValueError("Either 'token' ('JIRA_TOKEN' env variable) or both 'login' ('JIRA_LOGIN' env variable) and 'password' ('JIRA_PASSWORD' env variable) must be provided.")


jira_document_reader = JiraDocumentReader(base_url=args['url'],
                                          query=args['jql'],
                                          token=token,
                                          login=login, 
                                          password=password)

jira_document_converter = JiraDocumentConverter()

jira_collection_creator = create_collection_creator(collection_name=args['collection'],
                                                     indexers=args['indexers'],
                                                     document_reader=jira_document_reader,
                                                     document_converter=jira_document_converter)


jira_collection_creator.update_collection()