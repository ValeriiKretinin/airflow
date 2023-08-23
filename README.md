# Apache Airflow Repository

This repository houses workflows and operators for Apache Airflow, optimized for ClickHouse database operations.

## Repository Structure

- `airflow/dags`: This directory contains Directed Acyclic Graphs (DAGs) for different workflows.
- `airflow/operators`: Here you will find custom operators that extend Airflow's capabilities.

## Featured DAGs

### ClickHouse Backup DAGs

- **Weekly Full Backup (`clickhouse_backup_weekly`):** A DAG responsible for the weekly full backup of ClickHouse databases.

- **Daily Differential Backup (`clickhouse_backup_daily`):** A DAG that manages daily differential backups, skipping Sundays when a full backup is done.


## Custom Operators

The `operators` directory contains custom-built operators for specific needs. For instance:

- `RsyncOperator`: A sample operator for syncing data between systems. *(More details to be added as more operators are developed)*

## Getting Started

1. Install Apache Airflow.
2. Clone this repository.
3. Configure Airflow to recognize the `dags` and `operators` directories.
4. Ensure you have the required plugins and connections set up in your Airflow instance.

## Contribution

If you'd like to contribute, please fork the repository and use a feature branch. Pull requests are warmly welcome.

## Credits

Maintained by **Valerii Kretinin**.

