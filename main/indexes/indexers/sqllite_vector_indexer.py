import json
import os
import sqlite3
from typing import List, Tuple, Optional

import numpy as np
import sqlite_vec
from sqlite_vec import serialize_float32

from main.indexes.filter_parser import parse_filter
from main.indexes.sql_filter_converter import convert_filter_to_sql
from main.indexes.indexers.base_indexer import BaseIndexer
from main.indexes.embeddings.base_embedder import BaseEmbedder


class SqlliteVectorIndexer(BaseIndexer):
    __DB_FILE_NAME = "vector.db"
    __BATCH_SIZE = 500

    def __init__(self, name: str, embedder: BaseEmbedder, storage_path: str):
        self.name = name
        self.embedder = embedder
        self.__storage_path = storage_path
        self.__db_path = os.path.join(storage_path, self.__DB_FILE_NAME)
        self.__conn = None

    def get_name(self) -> str:
        return self.name

    def is_persistent_storage(self) -> bool:
        return True

    def index_texts(self, ids: np.ndarray, texts: List[str], items_metadata: list[dict] = None) -> None:
        embeddings = self.embedder.embed(texts)
        document_rows = [
            (int(id_val), serialize_float32(embedding.astype(np.float32).tolist()))
            for id_val, embedding in zip(ids, embeddings)
        ]
        self.__execute_in_batches(
            "INSERT INTO documents(doc_id, embedding) VALUES (?, ?)", document_rows
        )

        if items_metadata:
            metadata_rows = [(int(id_val), json.dumps(meta)) for id_val, meta in zip(ids, items_metadata)]
            self.__execute_in_batches(
                "INSERT OR REPLACE INTO metadata(doc_id, data) VALUES (?, ?)", metadata_rows
            )

        self.__get_conn().commit()

    def remove_ids(self, ids: np.ndarray) -> None:
        int_ids = [(int(id_val),) for id_val in ids]
        self.__execute_in_batches("DELETE FROM documents WHERE doc_id = ?", int_ids)
        self.__execute_in_batches("DELETE FROM metadata WHERE doc_id = ?", int_ids)
        self.__get_conn().commit()

    def serialize(self) -> bytes:
        raise NotImplementedError("SqlliteVectorIndexer uses persistent storage, serialization is not needed")

    def search(self, text: str, number_of_results: int = 10, filter: Optional[str] = None) -> Tuple[np.ndarray, np.ndarray]:
        if self.get_size() == 0:
            return np.array([[]]), np.array([[]])

        query_embedding = serialize_float32(self.embedder.embed(text).astype(np.float32).tolist())
        filter_expression = parse_filter(filter)

        if filter_expression:
            where_clause, filter_params = convert_filter_to_sql(filter_expression)
            cursor = self.__get_conn().execute(
                "SELECT doc_id, distance "
                "FROM documents "
                "WHERE embedding MATCH ? "
                "AND k = ? "
                f"AND doc_id IN (SELECT doc_id FROM metadata WHERE {where_clause}) "
                "ORDER BY distance",
                (query_embedding, number_of_results, *filter_params)
            )
        else:
            cursor = self.__get_conn().execute(
                "SELECT doc_id, distance "
                "FROM documents "
                "WHERE embedding MATCH ? "
                "AND k = ? "
                "ORDER BY distance",
                (query_embedding, number_of_results)
            )

        results = cursor.fetchall()

        if not results:
            return np.array([[]]), np.array([[]])

        distances = [row[1] for row in results]
        ids = [row[0] for row in results]

        return np.array([distances]), np.array([ids])

    def get_size(self) -> int:
        cursor = self.__get_conn().execute("SELECT COUNT(*) FROM documents")
        return cursor.fetchone()[0]

    def support_metadata(self) -> bool:
        return True

    def __execute_in_batches(self, statement: str, rows: List[tuple]) -> None:
        for i in range(0, len(rows), self.__BATCH_SIZE):
            self.__get_conn().executemany(statement, rows[i:i + self.__BATCH_SIZE])

    def __get_conn(self) -> sqlite3.Connection:
        if self.__conn is None:
            os.makedirs(self.__storage_path, exist_ok=True)
            db_exists = os.path.exists(self.__db_path)
            self.__conn = self.__connect()
            if not db_exists:
                self.__create_tables(self.__conn)
        return self.__conn

    def __connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.__db_path)
        conn.enable_load_extension(True)
        sqlite_vec.load(conn)
        conn.enable_load_extension(False)
        return conn

    def __create_tables(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            "CREATE VIRTUAL TABLE documents USING vec0("
            "doc_id INTEGER PRIMARY KEY, "
            f"embedding FLOAT[{self.embedder.get_number_of_dimensions()}])"
        )
        conn.execute(
            "CREATE TABLE metadata (doc_id INTEGER PRIMARY KEY, data JSON)"
        )
        conn.commit()
