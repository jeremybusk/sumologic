```
python3 sumo-blob-query-client.py --container-name sumo-archive --blob-prefix <prefix that year directories are in i.e. _index_foo> --search-key-values "_raw:Configured from console by " --start-date "2022-10-07T00:00:00+00:00" --end-date "2022-10-09T00:00:00+00:00" --storage-account-url https://<storage account name>.blob.core.windows.net/ --storage-access-key <access key> --log-level INFO --display-values --compression gzip
```
