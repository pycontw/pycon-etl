# Maintenance Guide

## Disk Space

Currently, the disk space is limited, so please check the disk space before running any ETL jobs.

This section will be deprecated if we no longer encounter out-of-disk issues.

1. Find the largest folders:
    ```bash
    du -a /var/lib/docker/overlay2 | sort -n -r | head -n 20
    ```
2. Show the folder size:
    ```bash
    du -hs xxxx
    ```
3. Delete the large folders identified.
4. Check disk space:
    ```bash
    df -h
    ```

## Token Expiration

Some API tokens might expire, so please check them regularly.

## Year-to-Year Jobs

Please refer to [Dev Data Team - Year to Year Jobs - HackMD](https://hackmd.io/R417olqPQSWnQYY1Oc_-Sw?view) for more details.
