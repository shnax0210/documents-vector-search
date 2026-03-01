import json

from ..persisters.base_persister import BasePersister

class DocumentCollectionFetcher:
    def __init__(self, collection_name: str, persister: BasePersister):
        self.collection_name = collection_name
        self.__persister = persister
        self.__documents_by_url = None

    def fetch(self, id=None, url=None, start_line=1, end_line=200) -> dict:
        if not id and not url:
            raise ValueError("Either id or url must be provided")

        document = self.__load_document(id, url)

        lines = document["text"].splitlines()
        total_lines = len(lines)

        start_line = max(1, start_line)
        end_line = min(end_line, total_lines)

        selected_lines = lines[start_line - 1:end_line]

        return {
            "collection": self.collection_name,
            "id": document["id"],
            "url": document["url"],
            "metadata": document.get("metadata", {}),
            "startLine": start_line,
            "endLine": end_line,
            "totalLines": total_lines,
            "text": "\n".join(selected_lines),
        }

    def __load_document(self, id, url):
        if id:
            return self.__load_document_by_id(id)
        return self.__load_document_by_url(url)

    def __load_document_by_id(self, id):
        path = f"{self.collection_name}/documents/{id}.json"
        if not self.__persister.is_path_exists(path):
            raise FileNotFoundError(f"Document with id '{id}' not found in collection '{self.collection_name}'")
        return json.loads(self.__persister.read_text_file(path))

    def __load_document_by_url(self, url):
        if self.__documents_by_url is None:
            self.__documents_by_url = self.__build_url_index()

        if url not in self.__documents_by_url:
            raise FileNotFoundError(f"Document with url '{url}' not found in collection '{self.collection_name}'")

        document_path = self.__documents_by_url[url]
        return json.loads(self.__persister.read_text_file(document_path))

    def __build_url_index(self):
        mapping_path = f"{self.collection_name}/indexes/index_document_mapping.json"
        mapping = json.loads(self.__persister.read_text_file(mapping_path))

        url_index = {}
        for entry in mapping.values():
            url_index[entry["documentUrl"]] = entry["documentPath"]
        return url_index
