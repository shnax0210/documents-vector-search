import sqlite3
import numpy as np
from typing import List, Tuple, Optional


class SqlliteIndexer:
    def __init__(self, name: str, serialized_data: Optional[bytes] = None):
        self.name = name
        self.__conn = sqlite3.connect(":memory:")

        if serialized_data is not None:
            self.__conn.deserialize(serialized_data)
        else:
            self.__conn.execute(
                "CREATE VIRTUAL TABLE documents USING fts5(doc_id, content)"
            )
            self.__conn.commit()

    def get_name(self) -> str:
        return self.name

    def index_texts(self, ids: np.ndarray, texts: List[str], items_metadata: list[dict] = None):
        rows = [(str(int(id_val)), text) for id_val, text in zip(ids, texts)]
        self.__conn.executemany(
            "INSERT INTO documents(doc_id, content) VALUES (?, ?)", rows
        )
        self.__conn.commit()

    def remove_ids(self, ids: np.ndarray):
        str_ids = [str(int(id_val)) for id_val in ids]
        placeholders = ",".join("?" * len(str_ids))
        self.__conn.execute(
            f"DELETE FROM documents WHERE doc_id IN ({placeholders})", str_ids
        )
        self.__conn.commit()

    def serialize(self) -> bytes:
        return self.__conn.serialize()

    def search(self, text: str, number_of_results: int = 10, filter: Optional[str] = None) -> Tuple[np.ndarray, np.ndarray]:
        query = self.__prepare_query(text)

        cursor = self.__conn.execute(
            "SELECT doc_id, bm25(documents) as score "
            "FROM documents "
            "WHERE documents MATCH ? "
            "ORDER BY bm25(documents) "
            "LIMIT ?",
            (query, number_of_results)
        )

        results = cursor.fetchall()

        if not results:
            return np.array([[]]), np.array([[]])

        ids = [int(row[0]) for row in results]
        scores = [row[1] for row in results]

        return np.array([scores]), np.array([ids])

    def get_size(self) -> int:
        cursor = self.__conn.execute("SELECT COUNT(*) FROM documents")
        return cursor.fetchone()[0]

    def support_metadata(self) -> bool:
        return False

    def __prepare_query(self, text: str) -> str:
        words = text.split()
        if not words:
            return '""'
        escaped_words = ['"' + word.replace('"', '""') + '"' for word in words]
        return " OR ".join(escaped_words)
