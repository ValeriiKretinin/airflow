# Apache Airflow Custom Operators

This repository contains a collection of custom operators for Apache Airflow. Currently, it includes the `RsyncOperator`. More operators will be added in the future.

## RsyncOperator

### Description
The `RsyncOperator` is designed to perform `rsync` operations between two remote servers. This operator aids in synchronizing files or folders from a source to a destination SSH server. It comes with features that allow users to override the default `rsync` options and even run `rsync` in a privileged mode using `sudo`.

### Features

- **SSH Connection:** Establishes SSH connections using provided source and destination connection IDs.

- **Temporary Private Key Handling:** Has the capability to temporarily write the private key on the source server for the `rsync` operation and ensures its deletion post-operation.

- **Privileged Mode:** Offers the option to execute the `rsync` command in privileged mode.

- **Override Default Options:** Allows users to override default `rsync` options.

- **Exception Handling:** Handles exceptions gracefully, ensuring that any cleanup or necessary additional actions are executed.

### Usage

Initialize the `RsyncOperator` with the required parameters like source and destination connection IDs, paths, and other optional settings. Once set up, the operator will oversee the `rsync` operation when it's run within an Airflow DAG.

---

Stay tuned for updates on additional operators in the future!
