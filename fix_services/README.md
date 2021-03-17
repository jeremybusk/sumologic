# Prep env
From this directory

# Create venv or use another method you prefer
```
apt install python3-venv  # on ubuntu or yum if redhat
pythonon3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install paramiko paramiko pywinrm pylibsshext
```

.env  # Create .env file with your SECRETS.
```
SUMO_USERNAME=
SUMO_USERPASS=
WINRM_USERNAME=
WINRM_USERPASS=
SSH_USERNAME=
SSH_USERPASS=
```

# Run
```
./service-manager.py 
```

I will be upgrading this shortly when I have time. So these are just basic instructions but haven't written tests yet or tested in general.

# Exit venv environment
```
deactivate
```
