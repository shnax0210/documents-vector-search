import json
from datetime import datetime, timezone
from enum import Enum

from ..utils.progress_bar import wrap_generator_with_progress_bar
from ..utils.progress_bar import wrap_iterator_with_progress_bar
from ..utils.performance import log_execution_duration

class OPERATION_TYPE(Enum):
    CREATE = "create"
    UPDATE = "update"

class DocumentCollectionCreator:
    def __init__(self,
                 collection_name: str,
                 document_reader,
                 document_converter,
                 document_indexers,
                 persister,
                 operation_type: OPERATION_TYPE = OPERATION_TYPE.CREATE,
                 indexing_batch_size=500_000):
        self.operation_type = operation_type
        self.collection_name = collection_name
        self.document_reader = document_reader
        self.document_convertor = document_converter
        self.document_indexers = document_indexers
        self.persister = persister
        self.indexing_batch_size = indexing_batch_size

    def update_collection(self):
        if self.operation_type == OPERATION_TYPE.CREATE:
            self.__create_collection()
            return
        
        if self.operation_type == OPERATION_TYPE.UPDATE:
            self.__update_collection()
            return
        
        raise ValueError(f"Unknown operation type: {self.operation_type}")

    def __create_collection(self):
        self.persister.remove_folder(self.collection_name)
        self.persister.create_folder(self.collection_name)

        self.__prcoess_collection()

    def __update_collection(self):
        if not self.persister.is_path_exists(self.collection_name):
            raise Exception(f"Collection {self.collection_name} does not exist. Please create it first.")

        existing_manifest = json.loads(self.persister.read_text_file(self.__build_manifest_path()))

        self.__prcoess_collection(existing_manifest)

    def __prcoess_collection(self, existing_manifest=None):
        update_time = datetime.now(timezone.utc)
        log_execution_duration(lambda: self.__read_documents(),
                               identifier=f"Reading documents for collection: {self.collection_name}")
    
        last_modified_document_time, number_of_documents, number_of_chunks = log_execution_duration(lambda: self.__index_documents(),
                                                                                                    identifier=f"Indexing documents for collection: {self.collection_name}")
        
        self.__create_manifest_file(existing_manifest, 
                                    update_time, 
                                    last_modified_document_time, 
                                    number_of_documents, 
                                    number_of_chunks)


    def __read_documents(self):
        for document in wrap_generator_with_progress_bar(self.document_reader.read_all_documents(), self.document_reader.get_number_of_documents()):
            for converted_document in self.document_convertor.convert(document):
                document_path = f"{self.collection_name}/documents/{converted_document['id']}.json"
                self.persister.save_text_file(json.dumps(converted_document, indent=4), document_path)

    def __index_documents(self):
        index_mapping = {}
        reverse_index_mapping = {}
        current_index_item_id = 0

        last_modified_document_time = None
        number_of_documents = 0

        for document_file_names in wrap_iterator_with_progress_bar(self.__get_file_name_batches()):
            items_to_index = []
            index_item_ids = []

            for document_file_name in document_file_names:
                document_path = f"{self.collection_name}/documents/{document_file_name}"
                converted_document = json.loads(self.persister.read_text_file(document_path))

                modified_document_time = datetime.fromisoformat(converted_document["modifiedTime"])
                if last_modified_document_time is None or last_modified_document_time < modified_document_time:
                    last_modified_document_time = modified_document_time
                number_of_documents += 1

                for chunk_number in range(0, len(converted_document["chunks"])):
                    items_to_index.append(converted_document["chunks"][chunk_number]["indexedData"])
                    index_item_ids.append(current_index_item_id)

                    index_mapping[current_index_item_id] = {
                        "documentId": converted_document["id"],
                        "documentUrl": converted_document["url"],
                        "documentPath": document_path,
                        "chunkNumber": chunk_number
                    }

                    if converted_document["id"] not in reverse_index_mapping:
                        reverse_index_mapping[converted_document["id"]] = []
                    reverse_index_mapping[converted_document["id"]].append(current_index_item_id)

                    current_index_item_id += 1

            for indexer in self.document_indexers:
                indexer.index_texts(items_to_index, index_item_ids)

        for indexer in self.document_indexers:
            index_base_path = self.__build_index_base_path(indexer)

            self.persister.save_bin_file(indexer.serialize(), f"{index_base_path}/indexer")
            self.persister.save_text_file(json.dumps(index_mapping, indent=2), f"{index_base_path}/index_document_mapping.json")
            self.persister.save_text_file(json.dumps(reverse_index_mapping, indent=2), f"{index_base_path}/reverse_index_document_mapping.json")
        
        return last_modified_document_time, number_of_documents, current_index_item_id + 1

    def __build_index_base_path(self, indexer):
        return f"{self.collection_name}/indexes/{indexer.get_name()}"

    def __get_file_name_batches(self):
        file_names = self.persister.read_folder_files(f"{self.collection_name}/documents")

        return [file_names[i:i + self.indexing_batch_size] for i in range(0, len(file_names), self.indexing_batch_size)]

    def __create_manifest_file(self, 
                               existing_manifest, 
                               update_time, 
                               last_modified_document_time, 
                               number_of_documents, 
                               number_of_chunks):
        manifest_content = self.__create_manifest_content(existing_manifest, 
                                                          update_time, 
                                                          last_modified_document_time, 
                                                          number_of_documents, 
                                                          number_of_chunks)

        self.persister.save_text_file(json.dumps(manifest_content, indent=4), self.__build_manifest_path())

    def __build_manifest_path(self):
        return f"{self.collection_name}/manifest.json"

    def __create_manifest_content(self, existing_manifest, update_time, last_modified_document_time, number_of_documents, number_of_chunks):
        if existing_manifest:
            return { **existing_manifest,
                "updatedTime": update_time.isoformat(),
                "lastModifiedDocumentTime": last_modified_document_time.isoformat(),
                "numberOfDocuments": number_of_documents,
                "numberOfChunks": number_of_chunks,
            }

        return {
            "collectionName": self.collection_name,
            "updatedTime": update_time.isoformat(),
            "lastModifiedDocumentTime": last_modified_document_time.isoformat(),
            "numberOfDocuments": number_of_documents,
            "numberOfChunks": number_of_chunks,
            "reader": self.document_reader.get_reader_details(),
            "indexers": [{ "name": indexer.get_name() } for indexer in self.document_indexers],
        }
