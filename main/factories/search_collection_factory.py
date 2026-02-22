from main.persisters.disk_persister import DiskPersister
from main.indexes.indexer_factory import load_indexers
from main.core.documents_collection_searcher import DocumentCollectionSearcher

from main.utils.performance import log_execution_duration

def create_collection_searcher(collection_name, index_names, filter=None):
    return log_execution_duration(
        lambda: __create_collection_searcher(collection_name, index_names, filter),
        identifier=f"Preparing collection searcher"
    )

def __create_collection_searcher(collection_name, index_names, filter):
    disk_persister = DiskPersister(base_path="./data/collections")

    indexers = load_indexers(index_names, collection_name, disk_persister)
    
    return DocumentCollectionSearcher(collection_name=collection_name, 
                                      indexers=indexers, 
                                      persister=disk_persister,
                                      filter=filter)
