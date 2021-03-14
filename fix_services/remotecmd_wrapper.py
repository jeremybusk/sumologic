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
    elif test_tcp_port_open(host, 5986) == 0:
        return 5986, 'winrm'
    elif test_tcp_port_open(host, 5985) == 0:
        return 5985, 'winrm'
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


class RcmdClient:
    """Winrm and ssh wrapper class that currently uses pywinrm and paramiko"""
    # rshport, rshproto = get_remoteshell_port
    # def __init__(self, host, user, password):
    def __init__(self, host, username, userpass, transport='ntlm', server_cert_validation='validate'):
        self.rshport, self.shproto = get_remoteshell_port(host)
        if test_is_valid_host_or_ipaddr(host) != 0:
           print("ERROR: host is not resolvable or invalid ip addr.")
           return

        if self.rshport == ssh_port:
            client = paramiko.SSHClient()
            client.load_system_host_keys()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(host, username=username, password=userpass)
            self.client = client
            self.user = username
            self.password = userpass 
            self.host = host
        elif self.rshport == winrm_port_https or self.rshport == winrm_port_http:
            client = winrm.Session(host, auth=(username, userpass), transport=transport, server_cert_validation=server_cert_validation)
            self.client = client
            self.username = username
            self.userpass = userpass 
            self.transport = transport 
            self.server_cert_validation =  server_cert_validation 
        else:
            sys.exit('ERROR: Unsupported rsh port!')


    def close(self):
        if self.client is not None:
            self.client.close()
            self.client = None
    
    def execute(self, cmd, log=True, logfile='./ssh.log'):
        global ssh_port
        global winrm_port_http
        global winrm_port_https
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
