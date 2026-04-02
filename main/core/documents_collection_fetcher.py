import json

from ..persisters.base_persister import BasePersister

class DocumentCollectionFetcher:
    def __init__(self, collection_name: str, persister: BasePersister):
        self.collection_name = collection_name
        self.__persister = persister

    def fetch(self, id, start_line=1, end_line=200) -> dict:
        if not id:
            raise ValueError("id must be provided")

        document = self.__load_document_by_id(id)

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

    def __load_document_by_id(self, id):
        path = f"{self.collection_name}/documents/{id}.json"
        if not self.__persister.is_path_exists(path):
            raise FileNotFoundError(f"Document with id '{id}' not found in collection '{self.collection_name}'")
        return json.loads(self.__persister.read_text_file(path))
