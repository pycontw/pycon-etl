# Maintenance Guide

## Disk Space

Currently, the disk space is limited, so please check the disk space before running any ETL jobs.

Will deprecate this if we don't bump into out-of-disk issue any more.

1. Find topk biggest folders: `du -a /var/lib/docker/overlay2 | sort -n -r | head -n 20`
2. Show the folder size: `du -hs xxxx`
3. delete those pretty big folder
4. `df -h`

## Token expiration

Some api tokens might expire, please check.

## Year to Year Jobs

Please refer [Dev Data Team - Year to Year Jobs - HackMD](https://hackmd.io/R417olqPQSWnQYY1Oc_-Sw?view) for more details.
