#!/bin/bash
#
# export GOOGLE_APPLICATION_CREDENTIALS="<where to access service-account.json>"
#
root_pycon_etl=${HOME}/work-my-projects/pycontw-projects/PyCon-ETL
root_upload_data=${HOME}/work-my-projects/pycontw-projects/PyCon-ETL-working
project_id="pycontw-225217"
cmd_upload=${root_pycon_etl}/contrib/upload-kktix-ticket-csv-to-bigquery.py


for year in 2018 2019 2020
do
    for ticket_type in corporate individual reserved
    do
        suffix=${ticket_type}_attendees_${year}
        cmd_args="${root_upload_data}/${suffix}.csv -p ${project_id} -d ods -t ods_kktix_ticket_${suffix} --upload"
        echo ${cmd_args}
        ${cmd_upload} ${cmd_args}
    done
done
