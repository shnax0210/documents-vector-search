import os
from datetime import datetime, timedelta
import json

from main.persisters.disk_persister import DiskPersister
from main.sources.jira.jira_document_reader import JiraDocumentReader
from main.sources.jira.jira_document_converter import JiraDocumentConverter
from main.sources.jira.jira_cloud_document_reader import JiraCloudDocumentReader
from main.sources.jira.jira_cloud_document_converter import JiraCloudDocumentConverter
from main.sources.confluence.confluence_document_reader import ConfluenceDocumentReader
from main.sources.confluence.confluence_cloud_document_reader import ConfluenceCloudDocumentReader
from main.sources.confluence.confluence_document_converter import ConfluenceDocumentConverter
from main.sources.confluence.confluence_cloud_document_converter import ConfluenceCloudDocumentConverter
from main.sources.files.files_document_reader import FilesDocumentReader
from main.sources.files.files_document_converter import FilesDocumentConverter
from main.indexes.indexer_factory import load_indexer
from main.core.documents_collection_creator import DocumentCollectionCreator, OPERATION_TYPE

from main.utils.performance import log_execution_duration

def create_collection_updater(collection_name):
    return log_execution_duration(
        lambda: __create_collection_updater(collection_name),
        identifier=f"Preparing collection updater"
    )

def __create_collection_updater(collection_name):
    disk_persister = DiskPersister(base_path="./data/collections")

    if not disk_persister.is_path_exists(collection_name):
        raise Exception(f"Collection {collection_name} does not exist")

    manifest = json.loads(disk_persister.read_text_file(f"{collection_name}/manifest.json"))

    document_reader, document_converter = __create_reader_and_converter(manifest)

    document_indexers = [load_indexer(indexer["name"], collection_name, disk_persister) for indexer in manifest['indexers']]

    return DocumentCollectionCreator(collection_name=collection_name,
                                     document_reader=document_reader, 
                                     document_converter=document_converter, 
                                     document_indexers=document_indexers,
                                     persister=disk_persister,
                                     operation_type=OPERATION_TYPE.UPDATE)


def __calculate_update_time(manifest):
    return datetime.fromisoformat(manifest['lastModifiedDocumentTime']) - timedelta(days=1)

def __calculate_update_date(manifest):
    return __calculate_update_time(manifest).date()

def __create_reader_and_converter(manifest):
    if manifest['reader']['type'] == 'jira':
        return __create_jira_reader_and_converter(manifest)
    
    if manifest['reader']['type'] == 'jiraCloud':
        return __create_jira_cloud_reader_and_converter(manifest)
    
    if manifest['reader']['type'] == 'confluence':
        reader, converter = __create_confluence_reader_and_converter(manifest)
        return [reader, converter]
    
    if manifest['reader']['type'] == 'confluenceCloud':
        reader, converter = __create_confluence_cloud_reader_and_converter(manifest)
        return [reader, converter]
    
    if manifest['reader']['type'] == 'localFiles':
        reader, converter = __create_local_files_reader_and_converter(manifest)
        return [reader, converter]

    raise Exception(f"Unknown document reader type: {manifest['reader']['type']}")


def __create_jira_reader_and_converter(manifest):
    token = os.environ.get('JIRA_TOKEN')
    login = os.environ.get('JIRA_LOGIN')
    password = os.environ.get('JIRA_PASSWORD')

    update_date = __calculate_update_date(manifest).isoformat()
    query_addition = f'AND (created >= "{update_date}" OR updated >= "{update_date}")'

    reader = JiraDocumentReader(base_url=manifest['reader']['baseUrl'], 
                                    query=f"{manifest['reader']['query']} {query_addition}",
                                    token=token,
                                    login=login, 
                                    password=password, 
                                    batch_size=manifest['reader']['batchSize'])
    converter = JiraDocumentConverter()
    return reader,converter

def __create_jira_cloud_reader_and_converter(manifest):
    email = os.environ.get('ATLASSIAN_EMAIL')
    api_token = os.environ.get('ATLASSIAN_TOKEN')

    if not email or not api_token:
        raise ValueError("Both 'ATLASSIAN_EMAIL' and 'ATLASSIAN_TOKEN' environment variables must be provided for Jira Cloud.")

    update_date = __calculate_update_date(manifest).isoformat()
    query_addition = f'AND (created >= "{update_date}" OR updated >= "{update_date}")'

    reader = JiraCloudDocumentReader(base_url=manifest['reader']['baseUrl'], 
                                    query=f"{manifest['reader']['query']} {query_addition}",
                                    email=email,
                                    api_token=api_token, 
                                    batch_size=manifest['reader']['batchSize'])
    converter = JiraCloudDocumentConverter()
    return reader,converter

def __create_confluence_reader_and_converter(manifest):
    token = os.environ.get('CONF_TOKEN')
    login = os.environ.get('CONF_LOGIN')
    password = os.environ.get('CONF_PASSWORD')

    if not token and (not login or not password):
        raise ValueError("Either 'token' ('CONF_TOKEN' env variable) or both 'login' ('CONF_LOGIN' env variable) and 'password' ('CONF_PASSWORD' env variable) must be provided.")

    update_date = __calculate_update_date(manifest).isoformat()
    query_addition = f'AND (created >= "{update_date}" OR lastModified >= "{update_date}")'

    reader = ConfluenceDocumentReader(base_url=manifest['reader']['baseUrl'], 
                                          query=f"{manifest['reader']['query']} {query_addition}",
                                          token=token,
                                          login=login, 
                                          password=password, 
                                          batch_size=manifest['reader']['batchSize'],
                                          read_all_comments=manifest['reader']['readAllComments'],)
    converter = ConfluenceDocumentConverter()
    return reader,converter

def __create_confluence_cloud_reader_and_converter(manifest):
    email = os.environ.get('ATLASSIAN_EMAIL')
    api_token = os.environ.get('ATLASSIAN_TOKEN')

    if not email or not api_token:
        raise ValueError("Both 'ATLASSIAN_EMAIL' and 'ATLASSIAN_TOKEN' environment variables must be provided for Confluence Cloud.")

    update_date = __calculate_update_date(manifest).isoformat()
    query_addition = f'AND (created >= "{update_date}" OR lastModified >= "{update_date}")'

    reader = ConfluenceCloudDocumentReader(base_url=manifest['reader']['baseUrl'], 
                                          query=f"{manifest['reader']['query']} {query_addition}",
                                          email=email,
                                          api_token=api_token, 
                                          batch_size=manifest['reader']['batchSize'],
                                          read_all_comments=manifest['reader']['readAllComments'],)
    converter = ConfluenceCloudDocumentConverter()
    return reader,converter


def __create_local_files_reader_and_converter(manifest):
    reader_config = manifest['reader']
    
    base_path = reader_config['basePath']
    include_patterns = reader_config.get('includePatterns', [".*"])
    exclude_patterns = reader_config.get('excludePatterns', [])
    fail_fast = reader_config.get('failFast', False)

    update_time = __calculate_update_time(manifest)
    
    reader = FilesDocumentReader(base_path=base_path,
                                include_patterns=include_patterns,
                                exclude_patterns=exclude_patterns,
                                fail_fast=fail_fast,
                                start_from_time=update_time)
    converter = FilesDocumentConverter()
    return reader, converter