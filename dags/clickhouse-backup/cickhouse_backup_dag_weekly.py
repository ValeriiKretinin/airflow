from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from datetime import timedelta
from operators.rsyncoperator import RsyncOperator

default_args = {
    'owner': 'DevOpsTeam',
    'start_date': datetime(2023, 7, 4),
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Initializing configs for backup
configurations = [
    {
        "clickhouse": 'CH_NAME',
        "connection": 'SSH_CONNECTION_ID',
        "local_config": 'PATH_TO_CONFIG/gcs_c1_backup_config.yml',
        "remote_config": 'PATH_TO_CONFIG/gcs_c1_backup_config.yml',
    },
    {
        "clickhouse": 'CH_NAME',
        "connection": 'SSH_CONNECTION_ID',
        "local_config": 'PATH_TO_CONFIG/gcs_c1_backup_config.yml',
        "remote_config": 'PATH_TO_CONFIG/gcs_c1_backup_config.yml',
    },
    {
        "clickhouse": 'CH_NAME_TO_OTHER_STAND',
        "connection": 'SSH_CONNECTION_ID',
        "local_config": 'PATH_TO_CONFIG/gcs_c1_backup_config.yml',
        "remote_config": 'PATH_TO_CONFIG/gcs_c1_backup_config.yml',
    }
]

with DAG(
        'clickhouse_backup_weekly',
        default_args=default_args,
        schedule_interval='@weekly'  # Run weekly
) as dag:
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')

    # This for loop generates tasks according to the previously declared configuration
    for i, config in enumerate(configurations):
        with TaskGroup(group_id=f'clickhouse_{i}') as clickhouse_group:
            transfer_backup_config = SFTPOperator(  # Transfer configs for clickhouse-backup app
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
                command=
                f"""
                curl -L https://github.com/Altinity/clickhouse-backup/releases/latest/download/clickhouse-backup-linux-amd64.tar.gz | tar xz
                sudo build/linux/amd64/clickhouse-backup create_remote --config {config["remote_config"]}
                sudo rm -rf {config["remote_config"]}
                """,
                conn_timeout=12000,
                cmd_timeout=12000
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
                        command=
                        """
                        curl -L https://github.com/Altinity/clickhouse-backup/releases/latest/download/clickhouse-backup-linux-amd64.tar.gz | tar xz
                        export backup_name=$(sudo ls -t /var/lib/clickhouse/backup | head -n 1 | sed 's:/*$::')
                        sudo build/linux/amd64/clickhouse-backup restore "${backup_name}" --config /tmp/clone_dwh_backup_config.yml
                        """,
                        conn_timeout=12000,
                        cmd_timeout=12000
                    )

                    delete_all_local_backups_stage = SSHOperator(
                        task_id='delete_all_local_backups_stage',
                        ssh_conn_id='SSH_CONNECTION_DEST',
                        command=
                        """
                        sudo rm -rf /tmp/clone_dwh_backup_config.yml
                        for dir in $(sudo ls /var/lib/clickhouse/backup/);
                        do
                            backup_name=$(basename $dir)
                            sudo build/linux/amd64/clickhouse-backup delete local "${backup_name}"
                        done
                        """,
                        conn_timeout=12000,
                        cmd_timeout=12000
                    )

                    transfer_backup_by_rsync >> restore_backup_ch_stage >> delete_all_local_backups_stage
                    collect_and_push_backup_gcs >> clickhouse_dwh_group

            clickhouse_group >> end
