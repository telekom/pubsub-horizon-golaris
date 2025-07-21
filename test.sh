#!/bin/bash

TOKEN="$1"
URL="https://stargate-integration.test.dhei.telekom.de/horizon/events/v1"
DATA='{
  "id": "b5882acc-e40e-47c4-b767-079d310f1ec9",
  "source": "http://apihost/some/path/resource/1234",
  "specversion": "1.0",
  "type": "tardis.horizon.demo.aws.v1",
  "datacontenttype": "application/json",
  "dataref": "http://apihost/some/api/v1/resource/1234",
  "time": "2024-03-14T09:53:47.369Z",
  "data": {
    "orderNumber": "12345"
      }
}'
HEADERS=(
  -H "Authorization: Bearer $TOKEN"
  -H "Content-Type: application/json"
)

for i in $(seq 1 1000)
do
  echo "Sending request #$i"
  curl -X POST "${HEADERS[@]}" -d "$DATA" "$URL"
  echo ""  # newline for readability
done
