#!/bin/bash
set -eu
shopt -s expand_aliases
alias scurl="curl -s -b cookies.txt -c cookies.txt -H 'Content-type: application/json' -H 'Accept: application/json'"
offset=0
limit=10000

json_file=$1
url1="${API_ENDPOINT}search/jobs"
url2=$(scurl -X POST -T ${json_file} --user "${ACCESS_ID}:${ACCESS_KEY}" "$url1" | jq -r .link.href)
echo $url2
while true; do
  sleep 5
  url2_state=$(scurl -X GET  --user "$ACCESS_ID:$ACCESS_KEY" "$url2" | jq -r .state)
  echo "state: $url2_state"
  if [ "$url2_state" = "DONE GATHERING RESULTS" ]; then
    scurl -X GET  --user "$ACCESS_ID:$ACCESS_KEY" "$url2/messages?offset=$offset&limit=$limit" | jq
    exit
  else
    echo NOT DONE GATHERING RESULTS
  fi
done
