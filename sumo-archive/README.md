# README

## Storage Account Container for Blobs

By default, all blobs will be uploaded in the storage account container "sumo-archive". The container is for grouping of blobs that you can limit access to.

The upload "sumo-archive" container must be created before running commands.

## Compression Versions

Zstandard was created for performance gains. Uses zst file extention. It is prefered over gzip because of faster compression & decompression.

LZ4 is another option. GZIP is most widely supported but less performant.

## CLI examples

### Run sumo query and export files to blob.

```
./sumo-query-to-files.py --query "_index=otel_infrequent" --from "2025-05-15T16:09:46+00:00" --max-size 5 --prefix _index=otel_infrequent --upload
```

### Query blob data for information

```
./sumo-blob-query-client.py --container-name sumo-archive --blob-name-base _index=otel_infrequent --search-key-values _collector:mycollectorname "_raw:changed state to down" 
--from-time "2025-05-14T16:00:00+00:00" --to-time "2025-05-18T17:00:00+00:00"
```


## File json format example for json.zst

my-prefix_part1.json.zst
```
[
  {
    "map": {
      "_blockid": "-102426977502539866",
      "_collector": "mycollector",
      "_collectorid": "318402720",
      "_format": "t:none:o:0:l:0:p:millisSinceEpoch",
      "_loglevel": "",
      "_messagecount": "0",
      "_messageid": "-7774562166938556920",
      "_messagetime": "1747325286092",
      "_raw": "Line protocol on Interface GigabitEthernet6/0/40, changed state to up",
      "_receipttime": "1747325291138",
      "_size": "69",
      "_source": "fluent-forwarder",
      "_sourcecategory": "network/generic",
      "_sourcehost": "192.16.1.1",
      "_sourceid": "2405558693",
      "_sourcename": "mysource",
      "_view": "infrequent"
    }
  },
  {
    "map": {

```

# Security

You can use sha1 hashes for files in read-only or signed file to verify they haven't changed.

You can encrypt the json.zst to json.zst.enc files with very long key for universal portability.

Encryption will increase time & resource usage to retrieve.

# Cost Savings

Use an Azure Container Instance in same region as blob storage account.

Spin Azure Container Instance up only when needed.

Make sure you have enough RAM to support in-memory options for max file sizes.

Azure Function App is another option where you only pay when used.
