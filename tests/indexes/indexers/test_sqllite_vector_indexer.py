import os
import shutil

import numpy as np
import pytest

from main.indexes.indexers.sqllite_vector_indexer import SqlliteVectorIndexer
from main.indexes.embeddings.base_embedder import BaseEmbedder


class FakeEmbedder(BaseEmbedder):
    VECTORS = {
        "alpha": [1.0, 0.0, 0.0],
        "beta": [0.0, 1.0, 0.0],
        "gamma": [0.0, 0.0, 1.0],
    }

    def embed(self, text) -> np.ndarray:
        if isinstance(text, list):
            return np.array([self.VECTORS[item] for item in text], dtype=np.float32)
        return np.array(self.VECTORS[text], dtype=np.float32)

    def get_number_of_dimensions(self) -> int:
        return 3


@pytest.fixture
def storage_dir(tmp_path):
    path = str(tmp_path / "sqllite_vector_storage")
    yield path
    if os.path.exists(path):
        shutil.rmtree(path)


def index_all(indexer):
    indexer.index_texts(
        np.array([0, 1, 2]),
        ["alpha", "beta", "gamma"],
        items_metadata=[
            {"space": "FIRST", "lastModifiedAt": "2026-01-01"},
            {"space": "SECOND", "lastModifiedAt": "2026-02-01"},
            {"space": "SECOND", "lastModifiedAt": "2026-03-01"},
        ],
    )


class TestSqlliteVectorIndexerStorage:
    def test_creates_storage_directory_on_first_use(self, storage_dir):
        indexer = SqlliteVectorIndexer("test_indexer", FakeEmbedder(), storage_dir)
        assert not os.path.exists(storage_dir)

        index_all(indexer)

        assert os.path.isdir(storage_dir)
        assert indexer.get_size() == 3

    def test_is_persistent_storage_returns_true(self, storage_dir):
        indexer = SqlliteVectorIndexer("test_indexer", FakeEmbedder(), storage_dir)
        assert indexer.is_persistent_storage() is True

    def test_data_survives_reopening(self, storage_dir):
        indexer = SqlliteVectorIndexer("test_indexer", FakeEmbedder(), storage_dir)
        index_all(indexer)

        reopened_indexer = SqlliteVectorIndexer("test_indexer", FakeEmbedder(), storage_dir)
        assert reopened_indexer.get_size() == 3

    def test_remove_ids(self, storage_dir):
        indexer = SqlliteVectorIndexer("test_indexer", FakeEmbedder(), storage_dir)
        index_all(indexer)

        indexer.remove_ids(np.array([1]))

        assert indexer.get_size() == 2
        _, ids = indexer.search("beta", number_of_results=3)
        assert 1 not in ids[0]

    def test_serialize_is_not_supported(self, storage_dir):
        indexer = SqlliteVectorIndexer("test_indexer", FakeEmbedder(), storage_dir)
        with pytest.raises(NotImplementedError):
            indexer.serialize()


class TestSqlliteVectorIndexerSearch:
    def test_returns_nearest_document_first(self, storage_dir):
        indexer = SqlliteVectorIndexer("test_indexer", FakeEmbedder(), storage_dir)
        index_all(indexer)

        distances, ids = indexer.search("gamma", number_of_results=3)

        assert ids[0].tolist()[0] == 2
        assert distances[0][0] == pytest.approx(0.0)

    def test_limits_number_of_results(self, storage_dir):
        indexer = SqlliteVectorIndexer("test_indexer", FakeEmbedder(), storage_dir)
        index_all(indexer)

        _, ids = indexer.search("alpha", number_of_results=2)

        assert ids.shape == (1, 2)

    def test_empty_index(self, storage_dir):
        indexer = SqlliteVectorIndexer("test_indexer", FakeEmbedder(), storage_dir)

        _, ids = indexer.search("alpha")

        assert ids.shape == (1, 0)

    def test_filters_by_metadata(self, storage_dir):
        indexer = SqlliteVectorIndexer("test_indexer", FakeEmbedder(), storage_dir)
        index_all(indexer)

        _, ids = indexer.search("alpha", number_of_results=3, filter='space = "SECOND"')

        assert sorted(ids[0].tolist()) == [1, 2]

    def test_filters_by_multiple_conditions(self, storage_dir):
        indexer = SqlliteVectorIndexer("test_indexer", FakeEmbedder(), storage_dir)
        index_all(indexer)

        _, ids = indexer.search(
            "alpha",
            number_of_results=3,
            filter='space = "SECOND" and lastModifiedAt > "2026-02-15"',
        )

        assert ids[0].tolist() == [2]

    def test_filters_with_or_condition(self, storage_dir):
        indexer = SqlliteVectorIndexer("test_indexer", FakeEmbedder(), storage_dir)
        index_all(indexer)

        _, ids = indexer.search(
            "alpha",
            number_of_results=3,
            filter='space = "FIRST" or space = "SECOND"',
        )

        assert sorted(ids[0].tolist()) == [0, 1, 2]

    def test_filter_is_applied_before_nearest_neighbors_limit(self, storage_dir):
        indexer = SqlliteVectorIndexer("test_indexer", FakeEmbedder(), storage_dir)
        index_all(indexer)

        _, ids = indexer.search("alpha", number_of_results=1, filter='space = "SECOND"')

        assert ids.shape == (1, 1)

    def test_filter_without_matches(self, storage_dir):
        indexer = SqlliteVectorIndexer("test_indexer", FakeEmbedder(), storage_dir)
        index_all(indexer)

        _, ids = indexer.search("alpha", number_of_results=3, filter='space = "MISSING"')

        assert ids.shape == (1, 0)
