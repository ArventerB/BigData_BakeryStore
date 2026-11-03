"""
Script test nhanh Kafka Producer cua TV1.
Dung de kiem tra xem cau hinh (acks, linger, batch, compression) hoat dong chuan hay chua.
"""
import json
import time
from datetime import datetime

try:
    from kafka import KafkaProducer
except ImportError:
    print("[!] Chua cai dat thu vien 'kafka-python'. Ban co the cai bang: pip install kafka-python lz4")
    exit(1)

from producer_config import CRITICAL_PRODUCER_CONFIG

TOPIC_NAME = "bakery.orders"

def run_test():
    print("=" * 60)
    print("BAT DAU KIEM THU PRODUCER (TV1 - TUAN 2)")
    print(f"Ket noi toi Kafka: {CRITICAL_PRODUCER_CONFIG['bootstrap_servers']}")
    print(f"Cau hinh: acks={CRITICAL_PRODUCER_CONFIG['acks']}, linger_ms={CRITICAL_PRODUCER_CONFIG['linger_ms']}, compression={CRITICAL_PRODUCER_CONFIG['compression_type']}")
    print("=" * 60)

    producer = KafkaProducer(
        bootstrap_servers=CRITICAL_PRODUCER_CONFIG["bootstrap_servers"],
        acks=CRITICAL_PRODUCER_CONFIG["acks"],
        retries=CRITICAL_PRODUCER_CONFIG["retries"],
        batch_size=CRITICAL_PRODUCER_CONFIG["batch_size"],
        linger_ms=CRITICAL_PRODUCER_CONFIG["linger_ms"],
        compression_type=CRITICAL_PRODUCER_CONFIG["compression_type"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None
    )

    sample_orders = [
        {"order_id": f"ORD_TEST_{i}", "customer": f"Khach_Hang_{i}", "items": [{"cake": "Croissant", "qty": 2, "price": 30000}], "total": 60000, "created_at": datetime.utcnow().isoformat()}
        for i in range(1, 6)
    ]

    print(f"[*] Dang gui {len(sample_orders)} don hang mau vao topic '{TOPIC_NAME}'...\n")

    for order in sample_orders:
        key = order["order_id"]
        # Gui message kem key (Kafka se hash key de phan bo vao partition phu hop)
        future = producer.send(TOPIC_NAME, key=key, value=order)
        record_metadata = future.get(timeout=10)
        print(f" -> Da gui thanh cong {order['order_id']} | Partition: {record_metadata.partition} | Offset: {record_metadata.offset}")

    producer.flush()
    producer.close()
    print("\n[V] Hoan tat! Ban co the len Kafka UI (http://localhost:8080) de xem cac message nay.")

if __name__ == "__main__":
    run_test()

