# README

## Infrequent Tier Example (More concurrent search jobs - 200 max)

```
CMD="time python3 sumo-minutes-query-to-files.py --start-date 2025-01-01T00:00:00Z  --end-date 2025-06-01T00:00:00Z --query "_index=MyWinEvent_Security" --output-dir _index_MyWinEvent_Security --log-level INFO --max-concurrent-jobs 180"
nohup bash -c "$CMD" >> nohup.log 2>&1 &
tail -f nohup.log
```

## .gzip

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
