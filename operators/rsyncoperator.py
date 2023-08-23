import json
from airflow.models import BaseOperator, Connection
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.utils.decorators import apply_defaults

class RsyncOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            source_ssh_conn_id,
            dest_ssh_conn_id,
            source_path,
            dest_path,
            privileged=False,
            rsync_opts=None,
            temp_key_path="/tmp/temp_ssh_key",
            *args,
            **kwargs,
    ):
        """
        Initialize the RsyncOperator.

        Parameters:
        - source_ssh_conn_id: Connection ID for the source SSH connection.
        - dest_ssh_conn_id: Connection ID for the destination SSH connection.
        - source_path: Path on the source server to the files/folders to be synced.
        - dest_path: Path on the destination where the files/folders should be synced.
        - privileged: Whether to run the rsync command as sudo.
        - rsync_opts: Additional options for rsync command. -arq set as default
        - temp_key_path: Temporary path to store SSH private key.
        """
        super().__init__(*args, **kwargs)
        self.source_ssh_conn_id = source_ssh_conn_id
        self.dest_ssh_conn_id = dest_ssh_conn_id
        self.source_path = source_path
        self.dest_path = dest_path
        self.privileged = privileged
        self.temp_key_path = temp_key_path
        self.rsync_opts = rsync_opts or '-ar'

    def execute(self, context):
        """
        Execute the Rsync operation between source and destination using the provided connection details and paths.
        """
        source_hook = SSHHook(ssh_conn_id=self.source_ssh_conn_id)
        dest_login, dest_host = self._get_login_and_host_from_conn_id(self.dest_ssh_conn_id)
        private_key = self._get_private_key_from_conn_id(self.dest_ssh_conn_id)

        # Create temp key file securely on the remote host
        create_temp_key_command = f"echo '{private_key}' > {self.temp_key_path} && chmod 600 {self.temp_key_path}"
        self._run_ssh_command(source_hook, create_temp_key_command)

        rsync_command = f"rsync {self.rsync_opts} -e 'ssh -i {self.temp_key_path}' {self.source_path} {dest_login}@{dest_host}:{self.dest_path}"
        if self.privileged:
            rsync_command = f"sudo {rsync_command}"

        try:
            self._run_ssh_command(source_hook, rsync_command)
        except Exception as e:
            raise RuntimeError(f"Error during rsync: {e}")
        finally:
            # Delete the temp key file on the remote host
            delete_temp_key_command = f"rm -f {self.temp_key_path}"
            self._run_ssh_command(source_hook, delete_temp_key_command)

    def _run_ssh_command(self, hook, command):
        """
        Execute a command on a remote server using SSH.

        Parameters:
        - hook: The SSHHook instance.
        - command: The command to execute on the remote server.

        Returns:
        - The output of the command.

        Throws:
        - Exception: If the command execution fails.
        """
        _, stdout, stderr = hook.get_conn().exec_command(command)
        exit_status = stdout.channel.recv_exit_status()

        if exit_status != 0:
            error = stderr.read().decode('utf-8').strip()
            raise Exception(f"Error executing command: {command}. Exit Status: {exit_status}. Error: {error}")

        return stdout.read().decode('utf-8')

    def _get_login_and_host_from_conn_id(self, conn_id):
        """
        Retrieve login and host information from a given connection ID.

        Parameters:
        - conn_id: The connection ID.

        Returns:
        - A tuple containing login and host information.
        """
        connection = Connection.get_connection_from_secrets(conn_id)
        return connection.login, connection.host

    def _get_private_key_from_conn_id(self, conn_id):
        """
        Retrieve the private key from a given connection ID.

        Parameters:
        - conn_id: The connection ID.

        Returns:
        - The private key as a string.
        """
        connection = Connection.get_connection_from_secrets(conn_id)
        extras = json.loads(connection.extra)
        return extras.get("private_key")
