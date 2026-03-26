import os
import io
import shutil
import tarfile
import tempfile
import pickle

import numpy as np
import pytest

from main.indexes.indexers.chroma_indexer import ChromaIndexer
from main.indexes.embeddings.base_embedder import BaseEmbedder


class FakeEmbedder(BaseEmbedder):
    def __init__(self, dimensions=8):
        self.__dimensions = dimensions

    def embed(self, text) -> np.ndarray:
        if isinstance(text, list):
            return np.random.rand(len(text), self.__dimensions).astype(np.float32)
        return np.random.rand(self.__dimensions).astype(np.float32)

    def get_number_of_dimensions(self) -> int:
        return self.__dimensions


@pytest.fixture
def storage_dir(tmp_path):
    path = str(tmp_path / "chroma_storage")
    yield path
    if os.path.exists(path):
        shutil.rmtree(path)


class TestChromaIndexerNewStorage:
    def test_creates_storage_directory_on_first_use(self, storage_dir):
        indexer = ChromaIndexer("test_indexer", FakeEmbedder(), storage_dir)
        assert not os.path.exists(storage_dir)

        indexer.index_texts(
            np.array([0, 1]),
            ["hello world", "foo bar"],
            items_metadata=[{"source": "test"}, {"source": "test"}],
        )

        assert os.path.isdir(storage_dir)
        assert indexer.get_size() == 2

    def test_is_persistent_storage_returns_true(self, storage_dir):
        indexer = ChromaIndexer("test_indexer", FakeEmbedder(), storage_dir)
        assert indexer.is_persistent_storage() is True

    def test_data_survives_reopening(self, storage_dir):
        embedder = FakeEmbedder()
        indexer = ChromaIndexer("test_indexer", embedder, storage_dir)
        indexer.index_texts(
            np.array([0, 1]),
            ["hello world", "foo bar"],
            items_metadata=[{"source": "a"}, {"source": "b"}],
        )
        assert indexer.get_size() == 2

        indexer2 = ChromaIndexer("test_indexer", embedder, storage_dir)
        assert indexer2.get_size() == 2

    def test_remove_ids(self, storage_dir):
        indexer = ChromaIndexer("test_indexer", FakeEmbedder(), storage_dir)
        indexer.index_texts(
            np.array([0, 1, 2]),
            ["a", "b", "c"],
            items_metadata=[{"k": "1"}, {"k": "2"}, {"k": "3"}],
        )
        assert indexer.get_size() == 3

        indexer.remove_ids(np.array([1]))
        assert indexer.get_size() == 2

    def test_search_returns_results(self, storage_dir):
        indexer = ChromaIndexer("test_indexer", FakeEmbedder(), storage_dir)
        indexer.index_texts(
            np.array([0, 1]),
            ["hello world", "foo bar"],
            items_metadata=[{"source": "a"}, {"source": "b"}],
        )

        distances, ids = indexer.search("hello", number_of_results=2)
        assert ids.shape[1] == 2

    def test_search_empty_collection(self, storage_dir):
        indexer = ChromaIndexer("test_indexer", FakeEmbedder(), storage_dir)
        distances, ids = indexer.search("hello")
        assert ids.shape == (1, 0)

    def test_serialize_still_produces_archive(self, storage_dir):
        indexer = ChromaIndexer("test_indexer", FakeEmbedder(), storage_dir)
        indexer.index_texts(
            np.array([0]),
            ["test"],
            items_metadata=[{"k": "v"}],
        )

        data = indexer.serialize()
        assert data.startswith(b"CHROMA_ARCHIVE_V1\0")


class TestChromaIndexerLegacyMigration:
    def test_migrate_archive_format(self, storage_dir):
        temp_dir = tempfile.mkdtemp()
        try:
            import chromadb
            from chromadb.config import Settings

            client = chromadb.PersistentClient(path=temp_dir, settings=Settings(anonymized_telemetry=False))
            col = client.get_or_create_collection("documents", metadata={"hnsw:space": "l2"})

            embedder = FakeEmbedder()
            embeddings = embedder.embed(["test doc"]).tolist()
            col.add(ids=["0"], embeddings=embeddings, metadatas=[{"source": "legacy"}])
            del client

            magic = b"CHROMA_ARCHIVE_V1\0"
            archive_buffer = io.BytesIO()
            with tarfile.open(fileobj=archive_buffer, mode="w:gz") as tar:
                for root, _, files in os.walk(temp_dir):
                    for f in files:
                        fp = os.path.join(root, f)
                        arcname = os.path.relpath(fp, temp_dir)
                        tar.add(fp, arcname=arcname, recursive=False)
            legacy_data = magic + archive_buffer.getvalue()
        finally:
            shutil.rmtree(temp_dir)

        indexer = ChromaIndexer("test_indexer", embedder, storage_dir, serialized_data=legacy_data)
        assert indexer.get_size() == 1
        assert os.path.isdir(storage_dir)

    def test_migrate_pickle_format(self, storage_dir):
        embedder = FakeEmbedder()
        embeddings = embedder.embed(["doc1", "doc2"]).tolist()
        collection_data = {
            "ids": ["0", "1"],
            "embeddings": embeddings,
            "metadatas": [{"source": "old"}, {"source": "old"}],
        }
        legacy_data = pickle.dumps(collection_data)

        indexer = ChromaIndexer("test_indexer", embedder, storage_dir, serialized_data=legacy_data)
        assert indexer.get_size() == 2
        assert os.path.isdir(storage_dir)
