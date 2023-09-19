# Airflow DAG for PostgreSQL Backup to Google Cloud Storage

## Description

This DAG is designed to automate the process of backing up a PostgreSQL database and storing it in Google Cloud Storage (GCS). It runs daily, as specified by `schedule_interval='@daily'`.

## Table of Contents

- [Variables](#variables)
- [Tasks](#tasks)
    - [Backup and Archive](#backup-and-archive)
    - [Upload Backup to Google Cloud Storage](#upload-backup-to-google-cloud-storage)
    - [Remove Backup Files](#remove-backup-files)
- [Dependencies](#dependencies)
- [Default Arguments](#default-arguments)
- [Database Connection](#database-connection)

## Variables

- `GCS_BUCKET`: The name of the Google Cloud Storage bucket where backups will be uploaded.
- `SHARE_DIR`: A shared directory for temporary storage, fetched from Airflow Variables.
- `STAND`: Environment information, fetched from Airflow Variables.
- `TMP_DIR`: Directory for temporary data storage.
- `ARCHIVE_NAME`: Format of the archive name that includes the current date.
- `TMP_ARCHIVE_PATH`: Path where temporary archives are stored.
- `SHARE_ARCHIVE_PATH`: Path where the archive is moved after hashing.
- `BACKUP_DIR_NAME`: The directory name for backups.

## Tasks

### Backup and Archive

- **Task ID**: `backup_and_archive`
- **What it does**:
    - Performs a base backup of the PostgreSQL database using `pg_basebackup`.
    - Creates a tar-gzip archive of the backup.
    - Moves the archive to a shared directory.
    - Validates the integrity of the moved archive by checking SHA-256 hash before and after the move.

### Upload Backup to Google Cloud Storage

- **Task ID**: `upload_backup_to_gcs`
- **What it does**:
    - Uses `LocalFilesystemToGCSOperator` to upload the archive from the shared directory to a specified location in Google Cloud Storage.

### Remove Backup Files

- **Task ID**: `remove_backup_files`
- **What it does**:
    - Removes the backup files from both the temporary and shared directories.

## Dependencies

The tasks execute in the following order:
- `backup_and_archive` >> `upload_backup_to_gcs` >> `remove_backup_files`

## Default Arguments

- `start_date`: The start date for the DAG is one day ago.
- `retries`: Number of retries set to 0.
- `owner`: Owned by 'DevOpsTeam'.

## Database Connection

PostgreSQL connection information is retrieved from Airflow's (HashiCorp Vault) stored connections.

