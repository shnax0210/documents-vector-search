import json
from datetime import datetime, timezone
from enum import Enum
import numpy as np
import logging

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
        self.document_converter = document_converter
        self.document_indexers = document_indexers
        self.persister = persister
        self.indexing_batch_size = indexing_batch_size

    def run(self):
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

        update_time = datetime.now(timezone.utc)
        document_ids, number_of_expected_documents = log_execution_duration(lambda: self.__read_documents(),
                                                                            identifier=f"Reading documents for collection: {self.collection_name}")
        
        if len(document_ids) == 0:
            logging.warning(f"No documents found for collection creation, so it will be not created.")
            self.persister.remove_folder(self.collection_name)
            return
    
        last_modified_document_time, number_of_chunks = log_execution_duration(lambda: self.__index_documents_for_new_collection(document_ids),
                                                                               identifier=f"Indexing documents for collection: {self.collection_name}")
        
        manifest = self.__create_manifest_file(update_time, 
                                               last_modified_document_time,
                                               number_of_chunks)
        
        if number_of_expected_documents != len(document_ids):
            logging.warning(f"Expected number of documents: {number_of_expected_documents} does not match actual number of read documents: {len(document_ids)}. Usually it happens when an error occurs during document reading. Please check logs for more details.")
        
        logging.info(f"Collection successfully created: \n{json.dumps(manifest, indent=2, ensure_ascii=False)}")

    def __update_collection(self):
        if not self.persister.is_path_exists(self.collection_name):
            raise Exception(f"Collection {self.collection_name} does not exist. Please create it first.")

        manifest = json.loads(self.persister.read_text_file(self.__build_manifest_path()))

        update_time = datetime.now(timezone.utc)
        document_ids, number_of_expected_documents = log_execution_duration(lambda: self.__read_documents(),
                                                                            identifier=f"Reading documents for collection: {self.collection_name}")
        
        if len(document_ids) == 0:
            logging.warning(f"No documents found for collection update, so it will be not updated.")
            return
    
        last_modified_document_time, number_of_chunks = log_execution_duration(lambda: self.__index_documents_for_existing_collection(document_ids),
                                                                               identifier=f"Indexing documents for collection: {self.collection_name}")
        
        manifest = self.__create_manifest_file(update_time, 
                                               last_modified_document_time,
                                               number_of_chunks,
                                               existing_manifest=manifest)
        
        if number_of_expected_documents != len(document_ids):
            logging.warning(f"Expected number of documents: {number_of_expected_documents} does not match actual number of read documents: {len(document_ids)}. Usually it happens when an error occurs during document reading. Please check logs for more details.")
        
        logging.info(f"Collection successfully updated: \n{json.dumps(manifest, indent=2, ensure_ascii=False)}")

    def __read_documents(self):
        document_ids = []

        number_of_expected_documents = self.document_reader.get_number_of_documents()
        for document in wrap_generator_with_progress_bar(self.document_reader.read_all_documents(), 
                                                         number_of_expected_documents, 
                                                         progress_bar_name="Reading documents"):
            for converted_document in self.document_converter.convert(document):
                document_path = f"{self.collection_name}/documents/{converted_document['id']}.json"
                self.__save_json_file(converted_document, document_path)

                document_ids.append(converted_document["id"])

        return document_ids, number_of_expected_documents

    def __index_documents_for_new_collection(self, document_ids):
        index_mapping = {}
        reverse_index_mapping = {}
        last_index_item_id = -1

        return self.__add_documents_to_index(document_ids,
                                      index_mapping,
                                      reverse_index_mapping,
                                      last_index_item_id)

    def __index_documents_for_existing_collection(self, document_ids):
        index_mapping = json.loads(self.persister.read_text_file(self.__build_index_mapping_path()))
        reverse_index_mapping = json.loads(self.persister.read_text_file(self.__build_reverse_index_mapping_path()))
        index_info = json.loads(self.persister.read_text_file(self.__build_index_info_path()))
        last_index_item_id = index_info["lastIndexItemId"]

        self.__remove_documents_from_index(document_ids, index_mapping, reverse_index_mapping)

        return self.__add_documents_to_index(document_ids,
                                      index_mapping,
                                      reverse_index_mapping,
                                      last_index_item_id)

    def __add_documents_to_index(self, 
                          document_ids, 
                          index_mapping, 
                          reverse_index_mapping, 
                          last_index_item_id):

        last_modified_document_time = None

        for batch_document_ids in wrap_iterator_with_progress_bar(self.__batch_items(document_ids,
                                                                                     self.indexing_batch_size), 
                                                                  progress_bar_name="Indexing batches of batches"):
            items_to_index = []
            index_item_ids = []
            items_metadata = []

            for document_id in batch_document_ids:
                document_path = f"{self.collection_name}/documents/{document_id}.json"

                converted_document = json.loads(self.persister.read_text_file(document_path))

                modified_document_time = datetime.fromisoformat(self.__get_modified_time(converted_document))
                if last_modified_document_time is None or last_modified_document_time < modified_document_time:
                    last_modified_document_time = modified_document_time

                for chunk_number in range(0, len(converted_document["chunks"])):
                    last_index_item_id += 1

                    items_to_index.append(converted_document["chunks"][chunk_number]["indexedData"])
                    index_item_ids.append(last_index_item_id)
                    items_metadata.append(converted_document.get("metadata", None))

                    index_mapping[last_index_item_id] = {
                        "documentId": converted_document["id"],
                        "documentUrl": converted_document["url"],
                        "documentPath": document_path,
                        "chunkNumber": chunk_number
                    }

                    if converted_document["id"] not in reverse_index_mapping:
                        reverse_index_mapping[converted_document["id"]] = []
                    reverse_index_mapping[converted_document["id"]].append(last_index_item_id)

            for indexer in self.document_indexers:
                indexer.index_texts(index_item_ids, items_to_index, items_metadata=items_metadata)

        for indexer in self.document_indexers:
            self.persister.save_bin_file(indexer.serialize(), f"{self.__build_index_base_path(indexer)}/indexer")

        index_info = { "lastIndexItemId": last_index_item_id, }
        self.__save_json_file(index_info, self.__build_index_info_path())
        self.__save_json_file(index_mapping, self.__build_index_mapping_path())
        self.__save_json_file(reverse_index_mapping, self.__build_reverse_index_mapping_path())

        return last_modified_document_time, self.document_indexers[0].get_size()

    def __get_modified_time(self, converted_document):
        if "metadata" in converted_document and "lastModifiedAt" in converted_document["metadata"]:
            return converted_document["metadata"]["lastModifiedAt"]
        
        # It's for backwards compatibility
        if "modifiedTime" in converted_document:
            return converted_document["modifiedTime"]
        
        raise ValueError(f"Cannot determine modified time for document with id: {converted_document['id']}")

    def __remove_documents_from_index(self, document_ids, index_mapping, reverse_index_mapping):
        for batch_document_ids in wrap_iterator_with_progress_bar(self.__batch_items(document_ids, 
                                                                                     self.indexing_batch_size), 
                                                                  progress_bar_name="Cleaning batches of batches"):
            index_ids_to_remove = []

            for document_id in batch_document_ids:
                if document_id in reverse_index_mapping:
                    document_index_ids_to_remove = reverse_index_mapping[document_id]

                    index_ids_to_remove.extend(document_index_ids_to_remove)

                    for index_id in document_index_ids_to_remove:
                        del index_mapping[str(index_id)]
                    del reverse_index_mapping[document_id]
        
            for indexer in self.document_indexers:
                indexer.remove_ids(np.array(index_ids_to_remove))

    def __build_reverse_index_mapping_path(self):
        return f"{self.collection_name}/indexes/reverse_index_document_mapping.json"

    def __build_index_mapping_path(self):
        return f"{self.collection_name}/indexes/index_document_mapping.json"

    def __build_index_info_path(self):
        return f"{self.collection_name}/indexes/index_info.json"

    def __build_index_base_path(self, indexer):
        return f"{self.collection_name}/indexes/{indexer.get_name()}"

    def __batch_items(self, items, batch_size):
        return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

    def __create_manifest_file(self, 
                               update_time, 
                               last_modified_document_time, 
                               number_of_chunks,
                               existing_manifest=None):
        manifest_content = self.__create_manifest_content(update_time, 
                                                          last_modified_document_time,
                                                          number_of_chunks,
                                                          existing_manifest=existing_manifest)

        self.__save_json_file(manifest_content, self.__build_manifest_path())

        return manifest_content

    def __build_manifest_path(self):
        return f"{self.collection_name}/manifest.json"

    def __create_manifest_content(self,
                                  update_time, 
                                  last_modified_document_time,
                                  number_of_chunks,
                                  existing_manifest=None):
        number_of_documents = len(self.persister.read_folder_files(f"{self.collection_name}/documents"))

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
            "converter": self.document_converter.get_details(),
            "indexers": [{ "name": indexer.get_name() } for indexer in self.document_indexers],
        }
    
    def __save_json_file(self, content, file_path):
        self.persister.save_text_file(json.dumps(content, indent=2, ensure_ascii=False), file_path)
