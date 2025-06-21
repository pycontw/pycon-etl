"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the task logs to avoid those getting too big.
airflow trigger_dag --conf '[curly-braces]"maxLogAgeInDays":30[curly-braces]' airflow-log-cleanup
--conf options:
    maxLogAgeInDays:<INT> - Optional
"""

import logging
from datetime import datetime, timedelta
from string import Template

from airflow.configuration import conf
from airflow.sdk import Variable, dag, task

log_cleanup_tpl = Template(
    """
echo "Getting Configurations..."
BASE_LOG_FOLDER="$directory"
WORKER_SLEEP_TIME="$sleep_time"

sleep ${WORKER_SLEEP_TIME}s

MAX_LOG_AGE_IN_DAYS="$max_log_age_in_days"
ENABLE_DELETE="$enable_delete"
echo "Finished Getting Configurations"

echo "Configurations:"
echo "BASE_LOG_FOLDER:      '${BASE_LOG_FOLDER}'"
echo "MAX_LOG_AGE_IN_DAYS:  '${MAX_LOG_AGE_IN_DAYS}'"
echo "ENABLE_DELETE:        '${ENABLE_DELETE}'"

cleanup() {
    echo "Executing Find Statement: $1"
    FILES_MARKED_FOR_DELETE=`eval $1`
    echo "Process will be Deleting the following File(s)/Directory(s):"
    echo "${FILES_MARKED_FOR_DELETE}"
    echo "Process will be Deleting `echo "${FILES_MARKED_FOR_DELETE}" | \
            grep -v '^$' | wc -l` File(s)/Directory(s)"     \
            # "grep -v '^$'" - removes empty lines.
            # "wc -l" - Counts the number of lines
    echo ""
    if [ "${ENABLE_DELETE}" == "true" ];
    then
        if [ "${FILES_MARKED_FOR_DELETE}" != "" ];
        then
            echo "Executing Delete Statement: $2"
            eval $2
            DELETE_STMT_EXIT_CODE=$?
            if [ "${DELETE_STMT_EXIT_CODE}" != "0" ]; then
                echo "Delete process failed with exit code \
                        '${DELETE_STMT_EXIT_CODE}'"

                echo "Removing lock file..."
                rm -f $log_cleanup_process_lock_file
                if [ "${REMOVE_LOCK_FILE_EXIT_CODE}" != "0" ]; then
                    echo "Error removing the lock file. \
                            Check file permissions.\
                            To re-run the DAG, ensure that the lock file has been \
                            deleted ($log_cleanup_process_lock_file)
                    exit ${REMOVE_LOCK_FILE_EXIT_CODE}
                fi
                exit ${DELETE_STMT_EXIT_CODE}
            fi
        else
            echo "WARN: No File(s)/Directory(s) to Delete"
        fi
    else
        echo "WARN: You're opted to skip deleting the File(s)/Directory(s)!!!"
    fi
}

if [ ! -f $log_cleanup_process_lock_file ]; then
    echo "Lock file not found on this node! Creating it to prevent collisions..."
    touch $log_cleanup_process_lock_file
    CREATE_LOCK_FILE_EXIT_CODE=$?
    if [ "${CREATE_LOCK_FILE_EXIT_CODE}" != "0" ]; then
        echo "Error creating the lock file. \
                Check if the airflow user can create files under tmp directory. \
                Exiting..."
        exit ${CREATE_LOCK_FILE_EXIT_CODE}
    fi

    echo "Running Cleanup Process..."

    FIND_STATEMENT="find ${BASE_LOG_FOLDER}/*/* -type f -mtime +${MAX_LOG_AGE_IN_DAYS}"
    DELETE_STMT="${FIND_STATEMENT} -exec rm -f {} \;"
    cleanup "${FIND_STATEMENT}" "${DELETE_STMT}"
    CLEANUP_EXIT_CODE=$?

    FIND_STATEMENT="find ${BASE_LOG_FOLDER}/*/* -type d -empty"
    DELETE_STMT="${FIND_STATEMENT} -prune -exec rm -rf {} \;"
    cleanup "${FIND_STATEMENT}" "${DELETE_STMT}"
    CLEANUP_EXIT_CODE=$?

    FIND_STATEMENT="find ${BASE_LOG_FOLDER}/* -type d -empty"
    DELETE_STMT="${FIND_STATEMENT} -prune -exec rm -rf {} \;"
    cleanup "${FIND_STATEMENT}" "${DELETE_STMT}"
    CLEANUP_EXIT_CODE=$?

    echo "Finished Running Cleanup Process"

    echo "Deleting lock file..."
    rm -f $log_cleanup_process_lock_file
    REMOVE_LOCK_FILE_EXIT_CODE=$?
    if [ "${REMOVE_LOCK_FILE_EXIT_CODE}" != "0" ]; then
        echo "Error removing the lock file. Check file permissions. \
              To re-run the DAG, ensure that the lock file has been deleted ($log_cleanup_process_lock_file)."
        exit ${REMOVE_LOCK_FILE_EXIT_CODE}
    fi

else
    echo "Another task is already deleting logs on this worker node. Skipping it!"
    echo "If you believe you're receiving this message in error, \
            kindly check if $log_cleanup_process_lock_file exists and delete it."
    exit 0
fi
"""
)

WORKER_NUM = 1
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
    tags=["teamclairvoyant", "airflow-maintenance-dags"],
)
def airflow_log_cleanup():
    @task
    def setup_directoryies_to_delete() -> list[str]:
        base_log_folder = conf.get("logging", "base_log_folder").rstrip("/")
        if not base_log_folder or not base_log_folder.strip():
            raise ValueError(
                "Cannot find logging.base_log_folder. Kindly provide an appropriate directory path."
            )
        directories_to_delete = [base_log_folder]

        # The number of worker nodes you have in Airflow. Will attempt to run this
        # process for however many workers there are so that each worker gets its
        # logs cleared.
        enable_delete_child_log = Variable.get(
            "airflow_log_cleanup__enable_delete_child_log", "True"
        )
        logging.info(f"ENABLE_DELETE_CHILD_LOG:  {enable_delete_child_log}")
        if enable_delete_child_log.lower() == "true":
            # TODO: update to airflow 3 var
            child_process_log_directory = conf.get(
                "scheduler", "child_process_log_directory", fallback=""
            )
            if child_process_log_directory.strip():
                directories_to_delete.append(child_process_log_directory)
            else:
                logging.warning(
                    "Could not obtain CHILD_PROCESS_LOG_DIRECTORY from Airflow Configurations: "
                )
        return directories_to_delete

    @task.bash
    def log_cleanup(
        directory: str,
        sleep_time: int,
        max_log_age_in_days: int | None = None,
        enable_delete: bool = True,
    ) -> str:
        """
        :param max_log_age_in_days: Length to retain the log files if not already provided in the conf. If this
            is set to 30, the job will remove those files that are 30 days old or older

        :param enable_delete: Whether the job should delete the logs or not. Included if you want to
            temporarily avoid deleting the logs
        """
        if max_log_age_in_days is None:
            max_log_age_in_days = Variable.get(
                "airflow_log_cleanup__max_log_age_in_days", 3
            )

        return log_cleanup_tpl.substitute(
            directory=directory,
            sleep_time=sleep_time,
            max_log_age_in_days=max_log_age_in_days,
            enable_delete=str(enable_delete).lower(),
            log_cleanup_process_lock_file="/tmp/airflow_log_cleanup_worker.lock",
        )

    directory_to_delete = setup_directoryies_to_delete()
    log_cleanup.expand(
        directory=directory_to_delete,
        sleep_time=list(range(3, (WORKER_NUM + 1) * 3, 3)),
    )


airflow_log_cleanup()
