# Prep env

# Create venv or use another method you prefer
```
git clone https://github.com/jeremybusk/sumologic.git
cd fix_services
sudo apt install python3-venv  # on ubuntu or yum if redhat
python3 -m venv venv
source venv/bin/activate
pip install -U pip
pip install -r requirements.txt
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
