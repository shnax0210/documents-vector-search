import sqlite3
import json
import os
import numpy as np
from typing import List, Tuple, Optional

from main.indexes.filter_parser import parse_filter, FilterNode, FilterCondition, FilterGroup
from main.indexes.indexers.base_indexer import BaseIndexer


class SqlliteIndexer(BaseIndexer):
    __DB_FILE_NAME = "bm25.db"

    def __init__(self, name: str, storage_path: str, serialized_data: Optional[bytes] = None):
        self.name = name
        self.__storage_path = storage_path
        self.__db_path = os.path.join(storage_path, self.__DB_FILE_NAME)
        self.__conn = None

        if serialized_data is not None:
            self.__migrate_legacy_data(serialized_data)

    def is_persistent_storage(self) -> bool:
        return True

    def get_name(self) -> str:
        return self.name

    def index_texts(self, ids: np.ndarray, texts: List[str], items_metadata: list[dict] = None) -> None:
        rows = [(str(int(id_val)), text) for id_val, text in zip(ids, texts)]
        self.__get_conn().executemany(
            "INSERT INTO documents(doc_id, content) VALUES (?, ?)", rows
        )

        if items_metadata:
            metadata_rows = [(str(int(id_val)), json.dumps(meta)) for id_val, meta in zip(ids, items_metadata)]
            self.__get_conn().executemany(
                "INSERT OR REPLACE INTO metadata(doc_id, data) VALUES (?, ?)", metadata_rows
            )

        self.__get_conn().commit()

    def remove_ids(self, ids: np.ndarray) -> None:
        str_ids = [str(int(id_val)) for id_val in ids]
        batch_size = 500
        for i in range(0, len(str_ids), batch_size):
            batch = str_ids[i:i + batch_size]
            placeholders = ",".join("?" * len(batch))
            self.__get_conn().execute(
                f"DELETE FROM documents WHERE doc_id IN ({placeholders})", batch
            )
            self.__get_conn().execute(
                f"DELETE FROM metadata WHERE doc_id IN ({placeholders})", batch
            )
        self.__get_conn().commit()

    def serialize(self) -> bytes:
        raise NotImplementedError("SqlliteIndexer uses persistent storage, serialization is not needed")

    def search(self, text: str, number_of_results: int = 10, filter: Optional[str] = None) -> Tuple[np.ndarray, np.ndarray]:
        query = self.__prepare_query(text)
        filter_expression = parse_filter(filter)

        if filter_expression:
            where_clause, filter_params = self.__convert_filter_to_sql(filter_expression)
            cursor = self.__get_conn().execute(
                "SELECT doc_id, bm25(documents) as score "
                "FROM documents "
                "WHERE documents MATCH ? "
                f"AND doc_id IN (SELECT doc_id FROM metadata WHERE {where_clause}) "
                "ORDER BY bm25(documents) "
                "LIMIT ?",
                (query, *filter_params, number_of_results)
            )
        else:
            cursor = self.__get_conn().execute(
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
        cursor = self.__get_conn().execute("SELECT COUNT(*) FROM documents")
        return cursor.fetchone()[0]

    def support_metadata(self) -> bool:
        return True

    def __get_conn(self):
        if self.__conn is None:
            os.makedirs(self.__storage_path, exist_ok=True)
            db_exists = os.path.exists(self.__db_path)
            self.__conn = sqlite3.connect(self.__db_path)
            if not db_exists:
                self.__conn.execute(
                    "CREATE VIRTUAL TABLE documents USING fts5(doc_id UNINDEXED, content)"
                )
                self.__conn.execute(
                    "CREATE TABLE metadata (doc_id TEXT PRIMARY KEY, data JSON)"
                )
                self.__conn.commit()
        return self.__conn

    def __migrate_legacy_data(self, serialized_data: bytes):
        os.makedirs(self.__storage_path, exist_ok=True)
        with open(self.__db_path, "wb") as f:
            f.write(serialized_data)
        conn = self.__get_conn()
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='metadata'"
        )
        if cursor.fetchone() is None:
            conn.execute(
                "CREATE TABLE metadata (doc_id TEXT PRIMARY KEY, data JSON)"
            )
            conn.commit()

    def __convert_filter_to_sql(self, node: FilterNode):
        if isinstance(node, FilterCondition):
            return f"json_extract(data, '$.{node.field}') {node.operator} ?", [node.value]

        child_sqls = []
        params = []
        for child in node.children:
            child_sql, child_params = self.__convert_filter_to_sql(child)
            child_sqls.append(child_sql)
            params.extend(child_params)

        joiner = " AND " if node.logical_operator == "and" else " OR "
        return f"({joiner.join(child_sqls)})", params

    def __prepare_query(self, text: str) -> str:
        words = text.split()
        if not words:
            return '""'
        escaped_words = ['content : "' + word.replace('"', '""') + '"' for word in words]
        return " AND ".join(escaped_words)
