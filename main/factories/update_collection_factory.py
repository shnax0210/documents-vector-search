import os
from datetime import datetime, timedelta
import json

from main.persisters.disk_persister import DiskPersister
from main.sources.jira.jira_document_reader import JiraDocumentReader
from main.sources.jira.jira_document_converter import JiraDocumentConverter
from main.sources.confluence.confluence_document_reader import ConfluenceDocumentReader
from main.sources.confluence.confluence_document_converter import ConfluenceDocumentConverter
from main.indexes.indexer_factory import load_indexer
from main.core.documents_collection_creator import DocumentCollectionCreator, OPERATION_TYPE

def create_collection_updator(collection_name):
    disk_persister = DiskPersister(base_path="./data/collections")

    if not disk_persister.is_path_exists(collection_name):
        raise Exception(f"Collection {collection_name} does not exist")

    manifest = json.loads(disk_persister.read_text_file(f"{collection_name}/manifest.json"))

    document_reader, document_converter = __create_reader_and_converter(manifest)

    document_indexers = [load_indexer(indexer["name"], collection_name, disk_persister) for indexer in manifest['indexers']]

    confluence_collection_creator = DocumentCollectionCreator(collection_name=collection_name, 
                                                              document_reader=document_reader, 
                                                              document_converter=document_converter, 
                                                              document_indexers=document_indexers,
                                                              persister=disk_persister,
                                                              operation_type=OPERATION_TYPE.UPDATE)

    return confluence_collection_creator


def __calculate_update_date(manifest):
    last_modified_minus_day = datetime.fromisoformat(manifest['lastModifiedDocumentTime']) - timedelta(days=1)
    return last_modified_minus_day.date().isoformat()

def __create_reader_and_converter(manifest):
    if manifest['reader']['type'] == 'jira':
        token = os.environ.get('JIRA_TOKEN')
        login=os.environ.get('JIRA_LOGIN')
        password=os.environ.get('JIRA_PASSWORD')

        update_date = __calculate_update_date(manifest)
        query_addition = f'AND (created >= "{update_date}" OR updated >= "{update_date}")'

        reader = JiraDocumentReader(base_url=manifest['reader']['baseUrl'], 
                                    query=f"{manifest['reader']['query']} {query_addition}",
                                    token=token,
                                    login=login, 
                                    password=password, 
                                    batch_size=manifest['reader']['batchSize'])
        converter = JiraDocumentConverter()

        return [reader, converter]
    
    if manifest['reader']['type'] == 'confluence':
        token = os.environ.get('CONF_TOKEN')
        login=os.environ.get('CONF_LOGIN')
        password=os.environ.get('CONF_PASSWORD')

        if not token and (not login or not password):
            raise ValueError("Either 'token' ('CONF_TOKEN' env variable) or both 'login' ('CONF_LOGIN' env variable) and 'password' ('CONF_PASSWORD' env variable) must be provided.")

        update_date = __calculate_update_date(manifest)
        query_addition = f'AND (created >= "{update_date}" OR lastModified >= "{update_date}")'

        reader = ConfluenceDocumentReader(base_url=manifest['reader']['baseUrl'], 
                                          query=f"{manifest['reader']['query']} {query_addition}",
                                          token=token,
                                          login=login, 
                                          password=password, 
                                          batch_size=manifest['reader']['batchSize'],
                                          read_comments=manifest['reader']['readComments'],)
        converter = ConfluenceDocumentConverter()

        return [reader, converter]

    raise Exception(f"Unknown document reader type: {manifest['type']}")
