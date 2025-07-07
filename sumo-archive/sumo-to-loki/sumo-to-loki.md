```
#!/bin/bash
set -eu
python3 sumo-to-loki.py \
  --loki-push-url $LOKI_URL \
  --tenant $ORG_ID \
  --basic-auth-user $LOKI_USER \
  --basic-auth-pass $LOKI_PASS \
  --message-files-dir ../by_minute/_index_foo \
  --compression-format gz \
  --message-files-format minute \
  --skip-tls-verify \
  --from-datetime 2022-10-10T12:00:00Z \
  --to-datetime 2025-07-01T00:00:00Z \
  --add-labels app=firewall,job=sumo_archive,archive=sumo \
  --remove-labels "_raw","_receipttime","_messagetime","_searchabletime","_size","_sourceid","_messagecount","_format","_collectorid","_blockid","_messagecount","_messageid" \
  --log-file sumo-to-loki.log \
  --log-level DEBUG \
  --concurrency 1 >> asa.sumo-to-loki.nohup.log 2>&1 &
```
