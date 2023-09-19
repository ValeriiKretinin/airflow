from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.dates import days_ago
from datetime import datetime
import json
from airflow.models import Variable

current_time = datetime.now().strftime("%Y-%m-%d")
DAG_ID = 'postgres_backup_to_gcs'
GCS_BUCKET = 'YOUR_BUCKET_NAME'
SHARE_DIR = Variable.get('share_dir') # if you are using share dirs
STAND = Variable.get('stand_env') # for dev, stage or prod from airflow variables or HashiCorp Vault
TMP_DIR = "/opt/airflow/data"
ARCHIVE_NAME = f"backup_{current_time}.tar.gz"
TMP_ARCHIVE_PATH = f"{TMP_DIR}/{ARCHIVE_NAME}"
SHARE_ARCHIVE_PATH = f"{SHARE_DIR}/{ARCHIVE_NAME}"
BACKUP_DIR_NAME = "backup"
TMP_BACKUP_PATH = f"{TMP_DIR}/{BACKUP_DIR_NAME}/"

default_args = {
    'start_date': days_ago(1),
    'retries': 0,
    'owner': 'DevOpsTeam'
}

with DAG(
        DAG_ID,
        schedule_interval='@daily',
        default_args=default_args,
        catchup=False,
) as dag:

    # Retrieve PostgreSQL connection information from Airflow
    connection = BaseHook.get_connection('postgres_backup')
    extras = json.loads(connection.extra) if connection.extra else {}
    host = connection.host[0] if isinstance(connection.host, list) else connection.host
    user = connection.login
    password = connection.password

    backup_and_archive_command = f""" 
    export PGPASSWORD={password}
    pg_basebackup  -U {user} -h {host} -D {TMP_BACKUP_PATH} -Fp -Xs -P
    tar -czf {TMP_ARCHIVE_PATH} -C {TMP_DIR} {BACKUP_DIR_NAME}
    # hash sum before mv
    ORIG_HASH=$(sha256sum {TMP_ARCHIVE_PATH} | awk '{{print $1}}')
    mv {TMP_ARCHIVE_PATH} {SHARE_ARCHIVE_PATH}
    # hash sum after mv
    NEW_HASH=$(sha256sum {SHARE_ARCHIVE_PATH} | awk '{{print $1}}')
    # Check that both hash are equal
    if [ "$ORIG_HASH" != "$NEW_HASH" ]; then
      echo "Hash mismatch, aborting!"
      exit 1
    fi
    """

    # Task to execute the backup, archiving and initial removal
    backup_and_archive = BashOperator(
        task_id='backup_and_archive',
        bash_command=backup_and_archive_command
    )

    # Task to upload the archived backup to Google Cloud Storage (GCS)
    upload_backup_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_backup_to_gcs',
        src=SHARE_ARCHIVE_PATH,
        dst=f"postgres/{STAND}/{ARCHIVE_NAME}",
        bucket=GCS_BUCKET,
        gcp_conn_id='gcs_backup_bucket',
    )

    remove_backup_command = f"rm -rf {SHARE_ARCHIVE_PATH} {TMP_BACKUP_PATH}"
    remove_backup_files = BashOperator(
        task_id='remove_backup_files',
        bash_command=remove_backup_command
    )

    backup_and_archive >> upload_backup_to_gcs >> remove_backup_files
