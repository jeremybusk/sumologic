# README

## CLI examples

```
./sumo-query-to-files.py --query "_index=otel_infrequent" --from "2025-05-15T16:09:46+00:00" --max-file-size 5 --verbose --short-filename --upload-direct-to-blob
```


```
./sumo-blob-query-client.py --container-name sumo-archive --blob-name-base _index=otel_infrequent --search-key-values _collector:foobar "_raw:changed state to down" 
--from-time "2025-05-14T16:00:00+00:00" --to-time "2025-05-18T17:00:00+00:00"
```
