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
SUMO_ACCESS_ID=<your access id from sumo cloud>
SUMO_ACCESS_KEY=<your access key from sumo cloud>>
USERNAME=<username with host perms>
USERPASS=<username pass>
```

# Run every 10 minutes
```
./service-manager.py -i 1800  # Uses .env values
./service-manager.py -i 1800 -u myuser -s mysumoaccessid  # Will prompt for password and key and you don't need .env file
```

OUtput is noisy now but the hosts it will try and restart service on look like but I don't test for failures yet
```
===============================
txd2-enrapply starting stopped service sumo-collector
================================
```

I will be upgrading this shortly when I have time. So these are just basic instructions but haven't written tests yet or tested in general.

# Exit venv environment
```
deactivate
```

And simple methods but lots of manual
```
invoke-command -computername ${host} -scriptblock {get-service sumo-collector}
invoke-command -computername ${host} -scriptblock {start-service sumo-collector}

ssh ${host} systemctl status collector
ssh ${host} systemctl start collector
```
