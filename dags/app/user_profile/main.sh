#!/bin/bash

python genai.py \
--file_path example_organization.txt \
--output_filename output_organization.csv \
--task_type organization

python genai.py \
--file_path example_job_title.txt \
--output_filename output_job_title.csv \
--task_type job_title
