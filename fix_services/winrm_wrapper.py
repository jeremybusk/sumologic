# powershell commands will be encoded by default for many reasons. If you want logged turn on logging https://github.com/ansible/ansible/issues/50107#issuecomment-448442954
# https://github.com/diyan/pywinrm
# Invoke-Expression ((New-Object System.Net.Webclient).DownloadString('https://raw.githubusercontent.com/ansible/ansible/devel/examples/scripts/ConfigureRemotingForAnsible.ps1'))

import winrm
import logging


class WinrmClient:
    """Winrm wrapper class that currently uses pywinrm"""

    def __init__(self, host, username, userpass, transport='ntlm', server_cert_validation='validate'):
        client = winrm.Session(host, auth=(username, userpass), transport=transport, server_cert_validation=server_cert_validation)
        self.client = client
        self.username = username
        self.userpass = userpass 
        self.transport = transport 
        self.server_cert_validation =  server_cert_validation 


    def close(self):
        if self.client is not None:
            self.client.close()
            self.client = None


    def execute(self, cmd, log=True, logfile='./winrm.log'):
        #logging.basicConfig(filename='/tmp/example.log', encoding='utf-8', level=logging.DEBUG)
        logging.basicConfig(handlers=[logging.FileHandler(filename=logfile,
            encoding='utf-8', mode='a+')],
            format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
            datefmt="%F %A %T", 
            level=logging.INFO)
        # Use "nohup" before cmds to keep shell scripts running after return.
        # rsp = self.client.run_ps(cmd)
        rsp = self.client.run_ps(cmd)
        exit_status = rsp.status_code
        out = rsp.std_out.decode()
        err = rsp.std_err.decode()
        if log:
            logging.info(cmd)

        rsp = {
            'exit_status': exit_status,
            'cmd': cmd,
            'out': out,
            'err': err
            }
        return rsp


def rpsh(host, cmd):
    if test_os_detect(host) == "windows":
        USERNAME = config('REMOTESHELL_USERNAME')
        USERPASS = config('REMOTESHELL_USERPASS')
        s = winrm.Session(host, auth=(USERNAME, USERPASS), transport='ntlm')
        r = s.run_ps(cmd)
        if r.status_code == 0:
            r.stdout = r.std_out.decode().strip()
            r.stderr = r.std_err.decode().strip()
            return r
        else:
            r.stdout = r.std_out.decode().strip()
            r.stderr = r.std_err.decode().strip()
            print(f"E: {r.stderr}")
            r.status = r.status_code
    elif test_os_detect(host) == "linux":
        print(f"{host} OS is Linux.")
        r = sshcmd(host, cmd)
        # if r.statuscode == 0:
        # print(vars(r))
        # print(r)
        print('aa')
        print(r['stdout'].decode())
        print('aa')
        if r.stdout:
            r.stdin = r.stdin.decode().strip()
            r.stdout = r.stdout.decode().strip()
            r.stderr = r.stderr.decode().strip()
            return r
        else:
            r.stdin = r.stdin.decode().strip()
            r.stdout = r.stdout.decode().strip()
            r.stderr = r.stderr.decode().strip()
            print(f"E: {r.stderr}")
            return r
        print("done")
    else:
        print(f"{host} OS is unsupported.")
        return

        return r
