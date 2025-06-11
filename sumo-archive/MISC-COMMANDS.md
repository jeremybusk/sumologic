# Commands

```
find dir/ f -size 48c
find /path/to/directory -type f -name "*.tmp"
```

## Install az cli with one liner

```
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
```

```
az storage blob upload-batch --account-name <my storage account name> --account-key <my storage access key> --source ./tmpout --destination sumo-archive/somefolder
```
