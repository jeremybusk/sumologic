# README

This is used for missing minutes/hours/days reports as well as aggregating compressed files to different compression format(lz4, gzip, zstd) or time files (hours or days instead of minutes).


## Example - minutes to days json.gz

This command will take messages stored in minutes files and aggregate to days files

```
./sumo-archive-tool --log-level debug aggregate --input-dir tmpin --output-dir tmpout --aggregate-from minute --aggregate-to day --output-compression gzip --input-compression gzip
```

## Install az cli with one liner

```
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash
```

## Upload tmpout to blob store via az cli

```
az storage blob upload-batch --account-name <my storage account name> --account-key <my storage access key> --source ./tmpout --destination sumo-archive/somefolder
```
