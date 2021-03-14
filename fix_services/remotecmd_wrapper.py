# powershell commands will be encoded by default for many reasons. If you want logged turn on logging https://github.com/ansible/ansible/issues/50107#issuecomment-448442954
# https://github.com/diyan/pywinrm
# Invoke-Expression ((New-Object System.Net.Webclient).DownloadString('https://raw.githubusercontent.com/ansible/ansible/devel/examples/scripts/ConfigureRemotingForAnsible.ps1'))

import logging

import winrm
import paramiko
import socket
import sys

ssh_port = 22
winrm_port_http = 5985 
winrm_port_https = 5986 


def get_os(host):
    if test_tcp_port_open(host, 5986) == 0:
        return "windows"
    elif test_tcp_port_open(host, 5985) == 0:
        return "windows"
    elif test_tcp_port_open(host, 22) == 0:
        return "linux"
    else:
        return "unknown"

def get_remoteshell_port(host):
    if test_tcp_port_open(host, 22) == 0:
        return 22, 'ssh'
        # return 22
    elif test_tcp_port_open(host, 5986) == 0:
        return 5986, 'winrm'
        # return 5986
    elif test_tcp_port_open(host, 5985) == 0:
        return 5985, 'winrm'
        # return 5985
    else:
        return "unknown", 'unknown'


def test_is_valid_host_or_ipaddr(host):
    try:
        ipaddress.ip_address(host)
        return 0
    except:
        pass
    try:
        socket.gethostbyname(host)
        return 0
    except:
        print(f"E: {host} is not valid ip address or can't be resolved!")
        return 1


def test_tcp_port_open(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1)
    address = (host, port)
    try:
        # r = s.connect_ex(address)
        s.connect((host, int(port)))
        s.shutdown(socket.SHUT_RDWR)
        s.close()
        return 0
    # sock.settimeout(None)
    except:
        return 1


class SshClient:

    def __init__(self, host, username, userpass):
        logfile='./ssh.log'
        paramiko.util.log_to_file(logfile)
        logging.basicConfig(handlers=[logging.FileHandler(filename=logfile,
            encoding='utf-8', mode='a+')],
            format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
            datefmt="%F %A %T", 
            level=logging.INFO)
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(host, username=username, password=userpass)
        self.client = client
        self.username = username
        self.userpass = userpass 

    def close(self):
        if self.client is not None:
            self.client.close()
            self.client = None

    # Use "nohup" before cmds to keep shell scripts running after return.
    # def execute(self, cmd):
    def execute(self, cmd, log=True, logfile='./ssh.log'):
        #if log:
            # logging.info(cmd)
	#    paramiko.util.log_to_file(logfile)
        use_password = False
        sudo = cmd.strip().split()[0]
        if sudo == 'sudo' and self.user != "root":
            # cmd = "sudo -S -p ' ' %s" % cmd
            cmd = f"sudo -S -p ' ' {cmd}"
            use_password = self.userpass is not None and len(self.userpass) > 0
        stdin, stdout, stderr = self.client.exec_command(cmd, get_pty=True)
        if use_password:
            stdin.write(self.userpass + "\n")
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


def remote_cmd():
    rshport = 5986 
    rshport = 22 
    if rshport == 22:
        s = SshClient('lxd0-sandbox', USERNAME, USERPASS)
        rsp = s.execute("ip address")
    elif rshport == 5986:
        cmd = 'ipconfig /all'
        s = WinrmClient('ws-w10', USERNAME, USERPASS, transport='ssl', server_cert_validation='ignore')
        rsp = s.execute(cmd)
    print(rsp['cmd'])
    print(rsp['out'])
    print(rsp['err'])
    print(rsp['exit_status'])


class RcmdClient:
    """Winrm and ssh wrapper class that currently uses pywinrm and paramiko"""
    # rshport, rshproto = get_remoteshell_port
    # def __init__(self, host, user, password):
    def __init__(self, host, username, userpass, transport='ntlm', server_cert_validation='validate'):
        # rshport = get_remoteshell_port
        # rshport, rshproto = get_remoteshell_port(host)
        self.rshport, self.shproto = get_remoteshell_port(host)
        # print(rshport)
        # print(rshport)
        # print(type(rshport))
        # rshport = 5986 
        # rshport = 22 
        if self.rshport == ssh_port:
            client = paramiko.SSHClient()
            client.load_system_host_keys()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(host, username=username, password=userpass)
            self.client = client
            self.user = username
            self.password = userpass 
            self.host = host
            # self.rshport = rshport 
            # self.rshproto = rshproto 
        elif self.rshport == winrm_port_https or self.rshport == winrm_port_http:
            client = winrm.Session(host, auth=(username, userpass), transport=transport, server_cert_validation=server_cert_validation)
            self.client = client
            self.username = username
            self.userpass = userpass 
            self.transport = transport 
            self.server_cert_validation =  server_cert_validation 
            # self.rshport = rshport 
            # self.rshproto = rshproto 
        else:
            sys.exit('ERROR: Unsupported rsh port!')


    def close(self):
        if self.client is not None:
            self.client.close()
            self.client = None
    
    def execute(self, cmd, log=True, logfile='./ssh.log'):
        if self.rshport == ssh_port:
            use_password = False
            sudo = cmd.strip().split()[0]
            if sudo == 'sudo' and self.user != "root":
                # cmd = "sudo -S -p ' ' %s" % cmd
                cmd = f"sudo -S -p ' ' {cmd}"
                use_password = self.userpass is not None and len(self.userpass) > 0
            stdin, stdout, stderr = self.client.exec_command(cmd, get_pty=True)
            if use_password:
                stdin.write(self.userpass + "\n")
                stdin.flush()

            rsp = {
                'exit_status': stdout.channel.recv_exit_status(),
                'cmd': cmd,
                'out': bytes.decode(stdout.read()),
                'err': bytes.decode(stderr.read())
                }
            return rsp
        elif self.rshport == winrm_port_https or self.rshport == winrm_port_http:
            #logging.basicConfig(filename='/tmp/example.log', encoding='utf-8', level=logging.DEBUG)
            # Use "nohup" before cmds to keep shell scripts running after return.
            # rsp = self.client.run_ps(cmd)
            logging.basicConfig(handlers=[logging.FileHandler(filename=logfile,
                encoding='utf-8', mode='a+')],
                format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                datefmt="%F %A %T", 
                level=logging.INFO)
            print('foo')
            print(cmd)
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


class WinrmClient:
    """Winrm and ssh wrapper class that currently uses pywinrm and paramiko"""

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
        # Use "nohup" before cmds to keep shell scripts running after return.
        # rsp = self.client.run_ps(cmd)
        logging.basicConfig(handlers=[logging.FileHandler(filename=logfile,
            encoding='utf-8', mode='a+')],
            format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
            datefmt="%F %A %T", 
            level=logging.INFO)
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
