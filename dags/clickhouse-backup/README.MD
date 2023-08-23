# ClickHouse Backup DAGs for Apache Airflow

This repository provides Apache Airflow Directed Acyclic Graphs (DAGs) for the backup operations of ClickHouse databases.

## Overview

We provide two main DAGs:

- **Weekly Full Backup (`clickhouse_backup_weekly`):**
    - Creates a full backup of the ClickHouse database.
    - Pushes backups to a Google Cloud Storage (GCS) bucket.
    - Has an additional flow to restore the backup on a different stand, if specified.

- **Daily Differential Backup (`clickhouse_backup_daily`):**
    - Creates differential backups from the last full backup.
    - Uses the latest directory in the GCS bucket to determine the differential changes.
    - Pushes backups to a GCS bucket.
    - Similar to the weekly backup, includes a flow to restore the backup on a different stand.

## Pre-requisites

- Apache Airflow with SSH and SFTP plugins installed.
- Access to the source and destination SSH servers.
- Configured GCS bucket for storing backups.
- `RsyncOperator` from the operators module.

## Configuration

Initialize configurations for your backup. Each configuration specifies the ClickHouse instance, SSH connection IDs, local and remote configurations for the backup.

## Usage

### Weekly Full Backup

Runs on a weekly basis. Operations include:
- Transferring configurations for the ClickHouse backup application.
- Collecting and pushing the ClickHouse backup to a GCS bucket.
- Restoring the backup on another ClickHouse instance (if specified).

### Daily Differential Backup

Runs daily except on Sundays. Operations include:
- Transferring configurations for the ClickHouse backup application.
- Using a Python function to retrieve the latest directory from the GCS bucket.
- Creating differential backups from the latest full backup.
- Pushing them to the GCS bucket.
- Restoring the differential backup on another ClickHouse instance (if specified).

## Monitoring & Failure Recovery

Both DAGs are designed with error handling and recovery:
- Handling SSH/SFTP connections gracefully.
- Executing cleanup or necessary actions after operations.
- Providing `retry_delay` and `retries` in `default_args` for retries.

## Future Enhancements

Consider the following as the system evolves:
- Implement alerts and notifications on backup failures.
- Incorporate database health checks post-restore.
- Enhance logging and monitoring.
- Explore automation for backup validation.

## Contributors

**DevOpsTeam**
