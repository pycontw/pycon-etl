#!/bin/bash
# Exit script on first error
set -e

# Check if the AIRFLOW_HOME variable is set
if [ -z "${AIRFLOW_HOME}" ]; then
  echo 'AIRFLOW_HOME not set'
  exit 1
fi

# Create Fernet key if not exists
if [ -z "${AIRFLOW__CORE__FERNET_KEY}" ]; then
  echo "Fernet key not set. Generating a new one."
  export AIRFLOW__CORE__FERNET_KEY=$(python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')
  echo "Fernet key generated and set."
else
  echo "Fernet key exists."
fi

# Check if the database exists and initialize it if not
if [ ! -f "${AIRFLOW_HOME}/airflow.db" ]; then
  airflow initdb
  echo 'Database initialized'
else
  echo 'Database existed'
fi

# Check if the command is provided
if [ -z "$1" ]; then
  echo "No command provided. Usage: $0 {airflow_command}"
  exit 1
fi

# Execute the provided Airflow command
echo "Running command: airflow $@"
exec airflow "$@"
