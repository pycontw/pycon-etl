#!/usr/bin/env python3
"""Pre-commit hook: ensure airflow version in pyproject.toml matches Dockerfile ARG."""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
PYPROJECT = ROOT / "pyproject.toml"
DOCKERFILES = [ROOT / "Dockerfile", ROOT / "Dockerfile.test"]


def get_pyproject_airflow_version() -> str | None:
    content = PYPROJECT.read_text()
    match = re.search(r'"apache-airflow==([^"]+)"', content)
    return match.group(1) if match else None


def get_dockerfile_airflow_version(path: Path) -> str | None:
    content = path.read_text()
    match = re.search(r"ARG AIRFLOW_VERSION=(\S+)", content)
    return match.group(1) if match else None


def main() -> int:
    pyproject_version = get_pyproject_airflow_version()
    if pyproject_version is None:
        print("ERROR: Could not find apache-airflow version in pyproject.toml")
        return 1

    failed = False
    for dockerfile in DOCKERFILES:
        if not dockerfile.exists():
            continue
        dockerfile_version = get_dockerfile_airflow_version(dockerfile)
        if dockerfile_version is None:
            print(f"ERROR: Could not find AIRFLOW_VERSION in {dockerfile.name}")
            failed = True
            continue
        if pyproject_version != dockerfile_version:
            print(
                f"ERROR: Airflow version mismatch!\n"
                f"  pyproject.toml : apache-airflow=={pyproject_version}\n"
                f"  {dockerfile.name}: ARG AIRFLOW_VERSION={dockerfile_version}\n"
                f"Please update them to the same version."
            )
            failed = True

    if not failed:
        print(f"OK: Airflow version is consistent ({pyproject_version})")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
