"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the task logs to avoid those getting too big.
airflow trigger_dag --conf '[curly-braces]"maxLogAgeInDays":30[curly-braces]' airflow-log-cleanup
--conf options:
    maxLogAgeInDays:<INT> - Optional
"""

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.exceptions import TaskDeferred
from airflow.providers.standard.triggers.temporal import (
    DateTimeTrigger,
)
from airflow.sdk import Param, Variable, chain, dag, task, task_group
from airflow.sdk.types import RuntimeTaskInstanceProtocol
from airflow.utils import timezone

if TYPE_CHECKING:
    pass

# get the airflow.task logger
task_logger = logging.getLogger("airflow.task")

lock_filename: str = "/tmp/airflow_log_cleanup_worker.lock"

default_args = {
    "owner": "operations",
    "depends_on_past": False,
    "email": ["davidtnfsh@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "start_date": datetime(2025, 6, 14),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["airflow-maintenance-dags"],
    params={
        "enable_delete_child_log": Param(True, type="boolean"),
    },
)
def airflow_log_cleanup():
    @task
    def find_directories_to_delete(params: dict[str, Any]) -> list[str]:
        base_log_folder = conf.get("logging", "base_log_folder").rstrip("/")
        if not base_log_folder or not base_log_folder.strip():
            raise ValueError(
                "Cannot find logging.base_log_folder in airflow configuration. "
                "Kindly provide an appropriate directory path."
            )
        directories_to_delete = [base_log_folder]

        enable_delete_child_log = params["enable_delete_child_log"]
        task_logger.info(f"ENABLE_DELETE_CHILD_LOG: {enable_delete_child_log}")
        if enable_delete_child_log is True:
            child_process_log_directory = conf.get(
                "logging", "dag_processor_child_process_log_directory", fallback=""
            )
            # Ensure the child process log directory is not part of the base_log_folder
            # since the children logs will be deleted along with parents
            if child_process_log_directory and (
                Path(base_log_folder) not in Path(child_process_log_directory).parents
            ):
                directories_to_delete.append(child_process_log_directory)
        return directories_to_delete

    @task_group
    def log_cleanup(
        directory: str,
        max_log_age_in_days: int | None = None,
        enable_delete: bool = True,
    ) -> None:
        """
        :param max_log_age_in_days: Length to retain the log files if not already provided in the conf. If this
            is set to 30, the job will remove those files that are 30 days old or older

        :param enable_delete: Whether the job should delete the logs or not. Included if you want to
            temporarily avoid deleting the logs
        """
        max_log_age_in_days = max_log_age_in_days or Variable.get(
            "airflow_log_cleanup__max_log_age_in_days", 3
        )

        @task
        def wait_for_n_seconds(ti: RuntimeTaskInstanceProtocol) -> None:
            """Wait for 10 * map_index seconds."""
            ti_index = ti.map_index or 0
            wait_time = ti_index * 10
            if wait_time:
                task_logger.info(f"Sleep for {wait_time} seconds.")
                target_dttm = timezone.utcnow() + timedelta(seconds=wait_time)
                raise TaskDeferred(
                    trigger=DateTimeTrigger(moment=target_dttm, end_from_trigger=True),
                    method_name="",
                )

        @task.short_circuit
        def check_process_lock_not_exists(filename: str) -> bool:
            if not os.path.isfile(filename):
                return True

            task_logger.warning(
                "Another task is already deleting logs on this worker node. Skipping it!"
                "If you believe you're receiving this message in error, "
                f"kindly check if {filename} exists and delete it."
            )
            return False

        @task
        def create_lock_file(filename: str) -> None:
            task_logger.info(
                "Lock file not found on this node! Creating it to prevent collisions"
            )
            try:
                with open(filename, "a"):
                    pass
            except OSError:
                task_logger.exception(
                    "Error creating the lock file. "
                    "Check if the airflow user can create files under tmp directory. "
                    "Exiting..."
                )
                raise

        @task_group
        def find_log_files_and_delete(
            directory: str,
            find_stmt_tpl: str,
            delete_stmt_tpl: str,
        ) -> None:
            @task.bash
            def exec_find_statement(find_stmt_tpl: str, directory: str) -> str:
                find_stmt = find_stmt_tpl % directory
                task_logger.info(f"Executing Find Statement: {find_stmt}")
                return find_stmt

            @task.short_circuit
            def check_whether_delete(enable_delete: bool, find_stmt_ret: str) -> bool:
                if enable_delete is False:
                    task_logger.warning(
                        "You're opted to skip deleting the File(s)/Directory(s)!!!"
                    )
                    return False

                if not find_stmt_ret:
                    task_logger.warning("No File(s)/Directory(s) to Delete")
                    return False

                number_of_files = len(find_stmt_ret.split())
                task_logger.info(
                    "Process will be Deleting the following File(s)/Directory(s):\n"
                    f"{find_stmt_ret}"
                    f"Process will be Deleting {number_of_files} File(s)/Directory(s)"
                )
                return True

            @task.bash
            def remove_log_files(
                directory: str, find_stmt_tpl: str, delete_stmt_tpl: str
            ) -> str:
                find_stmt = find_stmt_tpl % directory
                delete_stmt = delete_stmt_tpl % find_stmt
                task_logger.info(f"Executing Delete Statement: {delete_stmt}")
                return delete_stmt

            find_stmt_ret = exec_find_statement(
                directory=directory, find_stmt_tpl=find_stmt_tpl
            )
            chain(
                check_whether_delete(
                    enable_delete=enable_delete,
                    find_stmt_ret=find_stmt_ret,  # type: ignore[arg-type]
                ),
                remove_log_files(
                    directory=directory,
                    find_stmt_tpl=find_stmt_tpl,
                    delete_stmt_tpl=delete_stmt_tpl,
                ),
            )

        @task
        def remove_process_lock_file(lock_filename: str) -> None:
            task_logger.info("Removing lock file...")
            try:
                os.remove(lock_filename)
            except Exception:
                task_logger.exception(
                    "Error removing the lock file. Check file permissions. "
                    "To re-run the dag, ensure that the lock file {lock_filename} has been deleted"
                )
                raise

        chain(
            wait_for_n_seconds(),  # type: ignore[call-arg, misc]
            check_process_lock_not_exists(filename=lock_filename),
            create_lock_file(filename=lock_filename).as_setup(),
            find_log_files_and_delete.partial(directory=directory).expand_kwargs(
                [
                    {
                        # Find the files whose modification time was 3 days aao
                        "find_stmt_tpl": f"find %s/*/* -type f -mtime +{max_log_age_in_days}",
                        "delete_stmt_tpl": "%s -exec rm -f {} \;",
                    },
                    {
                        # Find the directory under base_log_folder/*/* that is empty
                        "find_stmt_tpl": "find %s/*/* -type d -empty",
                        "delete_stmt_tpl": "%s -prune -exec rm -rf {} \;",
                    },
                    {
                        # Find the directory under base_log_folder/* that is empty
                        "find_stmt_tpl": "find %s/* -type d -empty",
                        "delete_stmt_tpl": "%s -prune -exec rm -rf {} \;",
                    },
                ]
            ),
            remove_process_lock_file(lock_filename=lock_filename).as_teardown(),
        )

    directory_to_delete = find_directories_to_delete()
    log_cleanup.expand(directory=directory_to_delete)


airflow_log_cleanup()
