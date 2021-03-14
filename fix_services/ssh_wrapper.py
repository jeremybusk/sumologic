import paramiko

paramiko.util.log_to_file("paramiko.log")


class SshClient:

    def __init__(self, host, user, password):
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(host, username=user, password=password)
        self.client = client
        self.user = user
        self.password = password

    def close(self):
        if self.client is not None:
            self.client.close()
            self.client = None

    # Use "nohup" before cmds to keep shell scripts running after return.
    def execute(self, cmd):
        use_password = False
        sudo = cmd.strip().split()[0]
        if sudo == 'sudo' and self.user != "root":
            # cmd = "sudo -S -p ' ' %s" % cmd
            cmd = f"sudo -S -p ' ' {cmd}"
            use_password = self.password is not None and len(self.password) > 0
        stdin, stdout, stderr = self.client.exec_command(cmd, get_pty=True)
        if use_password:
            stdin.write(self.password + "\n")
            stdin.flush()

        r = {
            'exit_status': stdout.channel.recv_exit_status(),
            'cmd': cmd,
            'out': bytes.decode(stdout.read()),
            'err': bytes.decode(stderr.read())
            }
        return r
        # if usejson:
        #    json.loads(a)
