"""
Unit tests for user_profile UDF pure functions.
BigQuery / Airflow Variable / Gemini calls are NOT exercised here.
"""

import pytest

from dags.app.user_profile.udf import (
    JOB_REQUIREMENT,
    ORG_REQUIREMENT,
    chunk_data,
    get_task_config,
)


class TestChunkData:
    def test_exact_multiple(self) -> None:
        result = list(chunk_data([1, 2, 3, 4], 2))
        assert result == [[1, 2], [3, 4]]

    def test_remainder(self) -> None:
        result = list(chunk_data([1, 2, 3], 2))
        assert result == [[1, 2], [3]]

    def test_batch_larger_than_data(self) -> None:
        result = list(chunk_data([1, 2], 10))
        assert result == [[1, 2]]

    def test_empty_input(self) -> None:
        result: list[list[int]] = list(chunk_data([], 5))
        assert result == []

    def test_batch_size_one(self) -> None:
        result = list(chunk_data(["a", "b", "c"], 1))
        assert result == [["a"], ["b"], ["c"]]


class TestGetTaskConfig:
    def test_organization_returns_org_requirement(self) -> None:
        assert get_task_config("organization") == ORG_REQUIREMENT

    def test_job_title_returns_job_requirement(self) -> None:
        assert get_task_config("job_title") == JOB_REQUIREMENT

    def test_invalid_type_raises(self) -> None:
        with pytest.raises(ValueError, match="不支援的工作類型"):
            get_task_config("unknown_type")

    def test_org_requirement_contains_category_table(self) -> None:
        assert "產業類別對照表" in ORG_REQUIREMENT

    def test_job_requirement_contains_category_table(self) -> None:
        assert "職位類別對照表" in JOB_REQUIREMENT
