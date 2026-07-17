#!/usr/bin/env python3

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Sequence

DEFAULT_SOURCE_BRANCH = "master"
DEFAULT_TARGET_BRANCH = "prod"


def run(
    cmd: Sequence[str], *, check: bool = True, capture_output: bool = False
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=check, text=True, capture_output=capture_output)


def git_output(cmd: Sequence[str]) -> str:
    return subprocess.check_output(cmd, text=True).strip()


def ensure_git_repository() -> bool:
    try:
        run(["git", "rev-parse", "--git-dir"], capture_output=True)
    except subprocess.CalledProcessError:
        print(
            "Error: this command must be run inside a git repository.", file=sys.stderr
        )
        return False

    return True


def has_clean_working_tree() -> bool:
    return (
        run(["git", "diff", "--quiet"], check=False, capture_output=True).returncode
        == 0
        and run(
            ["git", "diff", "--cached", "--quiet"],
            check=False,
            capture_output=True,
        ).returncode
        == 0
    )


def ensure_clean_working_tree() -> bool:
    if not has_clean_working_tree():
        print(
            "Error: working tree is not clean. Commit or stash your changes first.",
            file=sys.stderr,
        )
        return False

    return True


def ensure_remote_branch_exists(branch: str) -> bool:
    try:
        run(["git", "rev-parse", "--verify", f"origin/{branch}"], capture_output=True)
    except subprocess.CalledProcessError:
        print(f"Error: origin/{branch} does not exist.", file=sys.stderr)
        return False

    return True


def checkout_branch(branch: str) -> None:
    current_branch = git_output(["git", "branch", "--show-current"])
    if current_branch != branch:
        run(["git", "checkout", branch])


def target_already_contains_source(source_branch: str) -> bool:
    return (
        run(
            ["git", "merge-base", "--is-ancestor", f"origin/{source_branch}", "HEAD"],
            check=False,
            capture_output=True,
        ).returncode
        == 0
    )


def confirm_promotion(source_branch: str, target_branch: str) -> bool:
    print(
        f"About to merge origin/{source_branch} into {target_branch} "
        f"and push origin/{target_branch}."
    )
    return input("Continue? [y/N] ").strip() in {"y", "Y"}


def promote(source_branch: str, target_branch: str) -> int:
    run(["git", "fetch", "origin", source_branch, target_branch])

    if not ensure_remote_branch_exists(source_branch):
        return 1
    if not ensure_remote_branch_exists(target_branch):
        return 1

    checkout_branch(target_branch)
    run(["git", "pull", "--ff-only", "origin", target_branch])

    if target_already_contains_source(source_branch):
        print(
            f"{target_branch} already contains origin/{source_branch}. Nothing to promote."
        )
        return 0

    if not confirm_promotion(source_branch, target_branch):
        print("Aborted.")
        return 1

    run(["git", "merge", f"origin/{source_branch}"])
    run(["git", "push", "origin", target_branch])

    print(f"Promotion completed: origin/{source_branch} -> origin/{target_branch}")
    return 0


def main() -> int:
    source_branch = os.environ.get("SOURCE_BRANCH", DEFAULT_SOURCE_BRANCH)
    target_branch = os.environ.get("TARGET_BRANCH", DEFAULT_TARGET_BRANCH)

    if not ensure_git_repository():
        return 1
    if not ensure_clean_working_tree():
        return 1

    return promote(source_branch, target_branch)


if __name__ == "__main__":
    raise SystemExit(main())
