from main.persisters.disk_persister import DiskPersister
from main.core.documents_collection_fetcher import DocumentCollectionFetcher


def create_collection_fetcher(collection_name):
    return DocumentCollectionFetcher(collection_name=collection_name, 
                                     persister=DiskPersister(base_path="./data/collections"))
