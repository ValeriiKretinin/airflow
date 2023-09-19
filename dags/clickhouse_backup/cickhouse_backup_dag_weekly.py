from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
from operators.rsyncoperator import RsyncOperator

# Constants
SSH_CONN_ID = 'SSH_CONNECTION_ID'
LOCAL_CONFIG_PATH = 'PATH_TO_CONFIG/gcs_c1_backup_config.yml'
REMOTE_CONFIG_PATH = 'PATH_TO_CONFIG/gcs_c1_backup_config.yml'
CH_BACKUP_URL = "https://github.com/Altinity/clickhouse-backup/releases/latest/download/clickhouse-backup-linux-amd64.tar.gz"
SSH_CONN_TIMEOUT = 12000
CMD_TIMEOUT = 12000

default_args = {
    'owner': 'DevOpsTeam',
    'start_date': datetime(2023, 7, 4),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Initializing configs for backup
configurations = [
    {
        "clickhouse": 'CH_NAME',
        "connection": SSH_CONN_ID,
        "local_config": LOCAL_CONFIG_PATH,
        "remote_config": REMOTE_CONFIG_PATH,
    },
    {
        "clickhouse": 'CH_NAME',
        "connection": SSH_CONN_ID,
        "local_config": LOCAL_CONFIG_PATH,
        "remote_config": REMOTE_CONFIG_PATH,
    },
    {
        "clickhouse": 'CH_NAME_TO_OTHER_STAND',
        "connection": SSH_CONN_ID,
        "local_config": LOCAL_CONFIG_PATH,
        "remote_config": REMOTE_CONFIG_PATH,
    },
]

with DAG(
        'clickhouse_backup_weekly',
        default_args=default_args,
        schedule_interval='@weekly'
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # This for loop generates tasks according to the previously declared configuration
    for i, config in enumerate(configurations):
        with TaskGroup(group_id=f'clickhouse_{i}') as clickhouse_group:
            transfer_backup_config = SFTPOperator(
                task_id=f'transfer_backup_config_{i}',
                ssh_conn_id=config["connection"],
                local_filepath=config["local_config"],
                remote_filepath=config["remote_config"],
                operation="put"
            )

            # Collect and push clickhouse backup to GCS Bucket
            collect_and_push_backup_gcs = SSHOperator(
                task_id=f'collect_and_push_backup_gcs_{i}',
                ssh_conn_id=config["connection"],
                command=f"""
                curl -L {CH_BACKUP_URL} | tar xz
                sudo build/linux/amd64/clickhouse-backup create_remote --config {config["remote_config"]}
                sudo rm -rf {config["remote_config"]}
                """,
                conn_timeout=SSH_CONN_TIMEOUT,
                cmd_timeout=CMD_TIMEOUT,
            )

            start >> transfer_backup_config >> collect_and_push_backup_gcs

            # Transfer and restoring the backup on clickhouse_clone
            if config["connection"] == 'CH_NAME_TO_OTHER_STAND':
                with TaskGroup(group_id=f'clickhouse_{i}_STAND') as clickhouse_dwh_group:
                    transfer_backup_by_rsync = RsyncOperator(
                        task_id="transfer_backup_by_rsync",
                        source_ssh_conn_id="SSH_CONNECTION_SOURCE",
                        dest_ssh_conn_id="SSH_CONNECTION_DEST",
                        source_path="/var/lib/clickhouse/backup/",
                        dest_path="/var/lib/clickhouse/backup/",
                        privileged=True,
                        rsync_opts='-rW --rsync-path="sudo rsync"',
                        dag=dag
                    )

                    restore_backup_ch_stage = SSHOperator(
                        task_id='restore_backup_ch_stage',
                        ssh_conn_id='SSH_CONNECTION_DEST',
                        command=f"""
                        curl -L {CH_BACKUP_URL} | tar xz
                        export backup_name=$(sudo ls -t /var/lib/clickhouse/backup | head -n 1 | sed 's:/*$::')
                        sudo build/linux/amd64/clickhouse-backup restore "${{backup_name}}" --config /tmp/clone_dwh_backup_config.yml
                        """,
                        conn_timeout=SSH_CONN_TIMEOUT,
                        cmd_timeout=CMD_TIMEOUT,
                    )

                    delete_all_local_backups_stage
