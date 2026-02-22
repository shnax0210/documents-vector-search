import json
import numpy as np


_RRF_K = 60


class DocumentCollectionSearcher:
    def __init__(self, collection_name, indexers, persister, filter=None):
        self.collection_name = collection_name
        self.__indexers = indexers
        self.__persister = persister
        self.__filter = filter

        for indexer in self.__indexers:
            if self.__filter and not indexer.support_metadata():
                raise NotImplementedError(f"Filter works only with indexers that support metadata (chromadb), {indexer.get_name()} does not support it.")

    def search(self, 
               text, 
               max_number_of_chunks=15, 
               max_number_of_documents=None, 
               include_text_content=False, 
               include_all_chunks_content=False, 
               include_matched_chunks_content=False):
        if len(self.__indexers) == 1:
            scores, indexes = self.__indexers[0].search(text, max_number_of_chunks, self.__filter)
        else:
            scores, indexes = self.__multi_index_search(text, max_number_of_chunks)

        results = self.__build_results(scores, indexes, include_text_content, include_all_chunks_content, include_matched_chunks_content)
        if max_number_of_documents:
            results = results[:max_number_of_documents]

        return {
            "collectionName": self.collection_name,
            "indexerName": ", ".join(indexer.get_name() for indexer in self.__indexers),
            "results": results,
        }

    def __multi_index_search(self, text, max_number_of_chunks):
        rrf_scores = {}

        for indexer in self.__indexers:
            scores, indexes = indexer.search(text, max_number_of_chunks, self.__filter)
            if len(indexes[0]) == 0:
                continue
            for rank, chunk_id in enumerate(indexes[0]):
                chunk_id = int(chunk_id)
                if chunk_id not in rrf_scores:
                    rrf_scores[chunk_id] = 0.0
                rrf_scores[chunk_id] += 1.0 / (_RRF_K + rank + 1)

        sorted_chunks = sorted(rrf_scores.items(), key=lambda x: x[1], reverse=True)
        sorted_chunks = sorted_chunks[:max_number_of_chunks]

        chunk_ids = [c[0] for c in sorted_chunks]
        chunk_scores = [c[1] for c in sorted_chunks]

        return np.array([chunk_scores]), np.array([chunk_ids])

    def __build_results(self, scores, indexes, include_text_content, include_all_chunks_content, include_matched_chunks_content):
        indexes_base_path = f"{self.collection_name}/indexes"
        index_document_mapping = json.loads(self.__persister.read_text_file(f"{indexes_base_path}/index_document_mapping.json"))

        result = {}

        for result_number in range(0, len(indexes[0])):
            mapping = index_document_mapping[str(indexes[0][result_number])]                   

            if mapping["documentId"] not in result:
                document = self.__get_document(mapping["documentPath"])
                result[mapping["documentId"]] = {
                    "id": mapping["documentId"],
                    "url": mapping["documentUrl"],
                    "path": mapping["documentPath"],
                    "lastModifiedAt":  document["metadata"]["lastModifiedAt"] if "metadata" in document else document.get("modifiedTime"),
                    "matchedChunks": [self.__build_chunk_result(mapping, scores, result_number, include_matched_chunks_content)]
                }

                if include_all_chunks_content or include_text_content:
                    if include_all_chunks_content:
                        result[mapping["documentId"]]["allChunks"] = document["chunks"]

                    if include_text_content:
                        result[mapping["documentId"]]["text"] = document["text"]

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
        return json.loads(self.__persister.read_text_file(documentPath))