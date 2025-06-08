from main.sources.document_cache_reader_decorator import CacheReaderDecorator
from main.core.documents_collection_creator import DocumentCollectionCreator, OPERATION_TYPE
from main.indexes.indexer_factory import create_indexer
from main.persisters.disk_persister import DiskPersister

from main.utils.performance import log_execution_duration

def create_collection_creator(collection_name, indexers, document_reader, document_converter, use_cache=True):
    return log_execution_duration(
        lambda: __create_collection_creator(collection_name, indexers, document_reader, document_converter, use_cache),
        identifier=f"Preparing collection creator"
    )

def __create_collection_creator(collection_name, indexers, document_reader, document_converter, use_cache):
    if use_cache:
        cache_disk_persister = DiskPersister(base_path="./data/caches")
        result_document_reader = CacheReaderDecorator(reader=document_reader,
                                                      persister=cache_disk_persister)
    else:
        result_document_reader = document_reader

    document_indexers = [create_indexer(indexer_name) for indexer_name in indexers]

    disk_persister = DiskPersister(base_path="./data/collections")

    return DocumentCollectionCreator(collection_name=collection_name, 
                                     document_reader=result_document_reader, 
                                     document_converter=document_converter,
                                     document_indexers=document_indexers,
                                     persister=disk_persister,
                                     operation_type=OPERATION_TYPE.CREATE)