#!/usr/bin/env bash
# Tạo các topic của tầng Ingestion. Chạy tự động bởi service init-topics trong docker-compose.
# Chạy tay:  docker compose run --rm init-topics
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka1:19092}"
PREFIX="${TOPIC_PREFIX:-shop}"
PARTITIONS="${PARTITIONS:-6}"
RF=3
KT=/opt/kafka/bin/kafka-topics.sh

echo "Chờ Kafka sẵn sàng tại $BOOTSTRAP ..."
until $KT --bootstrap-server "$BOOTSTRAP" --list > /dev/null 2>&1; do sleep 2; done

create() {
  local name=$1 parts=$2 retention_ms=$3
  $KT --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic "$name" --partitions "$parts" --replication-factor "$RF" \
      --config min.insync.replicas=2 \
      --config retention.ms="$retention_ms"
}

# retention: 7 ngày cho dữ liệu chính, 14 ngày cho DLQ để có thời gian điều tra lỗi
create "$PREFIX.clickstream" "$PARTITIONS" 604800000
create "$PREFIX.orders"      "$PARTITIONS" 604800000
create "$PREFIX.payments"    "$PARTITIONS" 604800000
create "$PREFIX.dlq"         3             1209600000

# Topic dùng riêng cho thí nghiệm số partition (benchmark/README)
for p in 1 3 6; do
  create "$PREFIX.bench.p$p" "$p" 86400000
done

echo "Danh sách topic:"
$KT --bootstrap-server "$BOOTSTRAP" --describe --topic "$PREFIX.clickstream"
$KT --bootstrap-server "$BOOTSTRAP" --list
