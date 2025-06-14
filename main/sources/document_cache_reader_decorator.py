import json
import hashlib
import logging

class CacheReaderDecorator:
    def __init__(self, reader, persister):
        self.reader = reader
        self.persister = persister

    def read_all_documents(self):
        cache_key = self.__build_cache_key()

        if self.persister.is_path_exists(cache_key) and self.persister.is_path_exists(f"{cache_key}_completed"):
            logging.info(f"Cache hit during 'read_all_documents' for {cache_key}")
            for file_name in self.persister.read_folder_files(cache_key):
                yield json.loads(self.persister.read_text_file(f"{cache_key}/{file_name}"))
        else:
            self.persister.remove_folder(cache_key)
            self.persister.create_folder(cache_key)
            document_index = -1
            for document in self.reader.read_all_documents():
                document_index += 1
                self.persister.save_text_file(json.dumps(document, indent=2, ensure_ascii=False), f"{cache_key}/{document_index}.json")

                yield document
            
            self.persister.save_text_file("", f"{cache_key}_completed")
    
    def get_number_of_documents(self):
        cache_key = self.__build_cache_key()
        
        if self.persister.is_path_exists(cache_key) and self.persister.is_path_exists(f"{cache_key}_completed"):
            logging.info(f"Cache hit during 'get_number_of_documents' for {cache_key}")
            return len(self.persister.read_folder_files(cache_key))
        else:
            return self.reader.get_number_of_documents()

    def get_reader_details(self) -> dict:
        return self.reader.get_reader_details()

    def remove_cache(self):
        cache_key = self.__build_cache_key()

        self.persister.remove_folder(cache_key)
        self.persister.remove_file(f"{cache_key}_completed")

    def __build_cache_key(self):
        hash_object = hashlib.sha256(json.dumps(self.reader.get_reader_details()).encode('utf-8')) 
        return hash_object.hexdigest()