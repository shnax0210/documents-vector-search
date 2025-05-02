import json

class DocumentCollectionSearcher:
    def __init__(self, collection_name, indexer, persister):
        self.collection_name = collection_name
        self.indexer = indexer
        self.persister = persister

    def search(self, text, number_of_results=10, include_text_content=False, include_all_chunks_content=False, include_matched_chunks_content=False):
        scores, indexes = self.indexer.search(text, number_of_results * 3)

        return {
            "collectionName": self.collection_name,
            "indexerName": self.indexer.get_name(),
            "results": self.__build_results(scores, indexes, include_text_content, include_all_chunks_content, include_matched_chunks_content)[:number_of_results],
        }

    def __build_results(self, scores, indexes, include_text_content, include_all_chunks_content, include_matched_chunks_content):
        indexes_base_path = f"{self.collection_name}/indexes"
        index_document_mapping = json.loads(self.persister.read_text_file(f"{indexes_base_path}/index_document_mapping.json"))

        result = {}

        for result_number in range(0, len(indexes[0])):
            mapping = index_document_mapping[str(indexes[0][result_number])]                   

            if mapping["documentId"] not in result:
                result[mapping["documentId"]] = {
                    "id": mapping["documentId"],
                    "url": mapping["documentUrl"],
                    "path": mapping["documentPath"],
                    "matchedChunks": [self.__build_chunk_result(mapping, scores, result_number, include_matched_chunks_content)]
                }

                if include_all_chunks_content or include_text_content:
                    dicument = self.__get_document(mapping["documentPath"])

                    if include_all_chunks_content:
                        result[mapping["documentId"]]["allChunks"] = dicument["chunks"]

                    if include_text_content:
                        result[mapping["documentId"]]["text"] = dicument["text"]

            else:
                result[mapping["documentId"]]["matchedChunks"].append(self.__build_chunk_result(mapping, scores, result_number, include_matched_chunks_content))
            
        return list(result.values())

    def __build_chunk_result(self, mapping, scores, result_number, include_matched_chunks_content):
        return {
            "chunkNumber": mapping["chunkNumber"],
            "score":  float(scores[0][result_number]),
            **({ "content": self.__get_document(mapping["documentPath"])["chunks"][mapping["chunkNumber"]] } if include_matched_chunks_content else {})
        }

    def __get_document(self, documentPath):
        return json.loads(self.persister.read_text_file(documentPath))