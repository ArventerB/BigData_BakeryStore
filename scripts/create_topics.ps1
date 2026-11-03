$KAFKA_CONTAINER = "kafka-1"
$BOOTSTRAP = "localhost:9092"
$RF = 3
Write-Host "--- DANG TAO CAC TOPIC CHO HE THONG TIEM BANH ---" -ForegroundColor Cyan
function Create-Topic($name, $partitions) {
    Write-Host "Dang tao topic: $name (partitions: $partitions, rf: $RF)..."
    docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server $BOOTSTRAP `
        --create --if-not-exists `
        --topic $name `
        --partitions $partitions `
        --replication-factor $RF `
        --config min.insync.replicas=2
}
# Tạo các topic nghiệp vụ chính
Create-Topic "bakery.user-events" 3
Create-Topic "bakery.orders" 3
Create-Topic "bakery.payments" 3
Create-Topic "bakery.dlq" 3

# 2. Cac topic phuc vu thi nghiem benchmark Tuan 3
Create-Topic "bakery.bench.p1" 1
Create-Topic "bakery.bench.p3" 3
Create-Topic "bakery.bench.p6" 6

Write-Host "`n--- DANH SACH TOPIC HIEN CO ---" -ForegroundColor Green
docker exec $KAFKA_CONTAINER /opt/kafka/bin/kafka-topics.sh --bootstrap-server $BOOTSTRAP --list