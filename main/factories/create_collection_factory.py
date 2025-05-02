from main.sources.document_cache_reader_decorator import CacheReaderDecorator
from main.core.documents_collection_creator import DocumentCollectionCreator, OPERATION_TYPE
from main.indexes.indexer_factory import create_indexer
from main.persisters.disk_persister import DiskPersister

def create_collection_creator(collection_name, indexers, document_reader, document_converter):
    cache_disk_persister = DiskPersister(base_path="./data/caches")

    document_cached_reader = CacheReaderDecorator(reader=document_reader, 
                                                             persister=cache_disk_persister)

    document_indexers = [create_indexer(indexer_name) for indexer_name in indexers]

    disk_persister = DiskPersister(base_path="./data/collections")

    return DocumentCollectionCreator(collection_name=collection_name, 
                                     document_reader=document_cached_reader, 
                                     document_converter=document_converter,
                                     document_indexers=document_indexers,
                                     persister=disk_persister,
                                     operation_type=OPERATION_TYPE.CREATE)