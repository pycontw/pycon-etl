"""Smoke test: every Dag module under ``dags/`` must import and parse cleanly."""

from __future__ import annotations

from pathlib import Path

import pytest
from airflow.models import DagBag

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dag_bag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dag_bag: DagBag) -> None:
    assert not dag_bag.import_errors, f"Dag import errors: {dag_bag.import_errors}"


def test_dags_loaded(dag_bag: DagBag) -> None:
    assert dag_bag.dags, "No Dags discovered under dags/"


def test_dag_ids_unique(dag_bag: DagBag) -> None:
    dag_ids = list(dag_bag.dag_ids)
    duplicates = {dag_id for dag_id in dag_ids if dag_ids.count(dag_id) > 1}
    assert not duplicates, f"Duplicate Dag IDs: {duplicates}"
