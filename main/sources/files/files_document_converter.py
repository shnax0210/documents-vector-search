import os

from main.sources.base_document_converter import BaseDocumentConverter
from main.splitter.base_text_splitter import BaseTextSplitter


class FilesDocumentConverter(BaseDocumentConverter):
    def __init__(self, text_splitter: BaseTextSplitter):
        self.__text_splitter = text_splitter

    def get_details(self) -> dict:
        return {
            "splitter": self.__text_splitter.get_details(),
        }

    def convert(self, document) -> list[dict]:
        return [{
            "id": document['fileRelativePath'],
            "url": self.__build_url(document),
            "metadata": {
                "createdAt": document['createdTime'],
                "lastModifiedAt": document['modifiedTime'],
                **self.__build_folder_metadata(document['fileRelativePath']),
            },
            "text": self.__build_document_text(document),
            "chunks": self.__split_to_chunks(document)
        }]
    
    def __build_document_text(self, document):
        content = self.__convert_to_text([content_part['text'] for content_part in document['content']], "")
        return self.__convert_to_text([document['fileRelativePath'], content])
    
    def __convert_to_text(self, elements, delimiter="\n\n"):
        return delimiter.join([element for element in elements if element]).strip()
    
    def __split_to_chunks(self, document):
        chunks = [{
                "indexedData": document['fileRelativePath']
            }]
        
        for content_part in document['content']:
            if content_part['text'].strip():
                for chunk in self.__text_splitter.split_text(content_part['text']):
                    chunks.append({
                        **({"metadata": content_part['metadata']} if "metadata" in content_part else {}),
                        "indexedData": chunk
                    })

            
        return chunks

    def __build_folder_metadata(self, path) -> dict:
        folders = os.path.dirname(path).split(os.sep)
        folders = [f for f in folders if f]
        return {f"folder{i + 1}": folder for i, folder in enumerate(folders)}

    def __build_url(self, document):
        return f"file://{document['fileFullPath']}"