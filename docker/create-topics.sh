#!/bin/sh
set -eu
for topic in activation.requested billing.events provisioning.events activation.events; do
 for name in "$topic" "$topic.dlq"; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --if-not-exists \
   --topic "$name" --partitions 3 --replication-factor 1 \
   --config retention.ms=604800000 --config segment.ms=3600000
 done
done
