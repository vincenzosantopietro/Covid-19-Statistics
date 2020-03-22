#!/bin/sh

docker run -d \
  --name docker-influxdb-grafana \
  -p 3003:3003 \
  -p 3004:8083 \
  -p 8086:8086 \
  -v influxdb:/var/lib/influxdb \
  -v grafana:/var/lib/grafana \
  philhawthorne/docker-influxdb-grafana:latest

#admin username: root
#admin psw: password
docker exec -it docker-influxdb-grafana grafana-cli admin reset-admin-password password

