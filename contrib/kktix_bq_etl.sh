#!/bin/bash
#
# export GOOGLE_APPLICATION_CREDENTIALS="<where to access service-account.json>"
#
project_id="pycontw-225217"
cmd=${PWD}/kktix_bq_etl.py


for ticket_type in corporate individual reserved
do
    suffix=${ticket_type}_attendees_test
    cmd_args="-p ${project_id} -d ods -t ods_kktix_ticket_${suffix} -k ${ticket_type} -y $1 --upload"
    echo ${cmd_args}
    ${cmd} ${cmd_args}
done