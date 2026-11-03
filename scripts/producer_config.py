"""
Module quy dinh cau hinh Kafka Producer chuan cho du an Bakery Store.
TV2 (Collector API) va TV3 (Simulator) dung chung cau hinh nay de dam bao:
- Tinh toan ven du lieu (khong mat don hang)
- Toi uu throughput (toc do truyen tai cao khi chay simulator lon)
"""

# Danh sach Broker (chay tu may host)
BOOTSTRAP_SERVERS_HOST = [
    "localhost:9092",
    "localhost:9094",
    "localhost:9096"
]

# Danh sach Broker (chay ben trong Docker container cung mang kafka-net)
BOOTSTRAP_SERVERS_DOCKER = [
    "kafka-1:29092",
    "kafka-2:29092",
    "kafka-3:29092"
]

# Cau hinh Producer chuan cho nghiep vu Don hang va Thanh toan (Yeu cau do tin cay cao)
CRITICAL_PRODUCER_CONFIG = {
    "bootstrap_servers": BOOTSTRAP_SERVERS_HOST,
    "acks": "all",                      # Cho tat ca cac in-sync replicas ghi thanh cong
    "retries": 5,                       # Tu dong thu lai 5 lan neu xay ra loi mang tam thoi
    "max_in_flight_requests_per_connection": 1,  # Dam bao thu tu tin nhan khong bi dao lon khi retry
    "batch_size": 65536,                # 64 KB
    "linger_ms": 10,                    # Cho toi da 10ms de gom batch tin nhan
    "compression_type": "lz4",          # Nen du lieu de giam tai mang (lz4 toc do cao, CPU nhe)
}

# Cau hinh Producer cho Clickstream/User Events (Yeu cau toc do sinh event cuc cao)
HIGH_THROUGHPUT_PRODUCER_CONFIG = {
    "bootstrap_servers": BOOTSTRAP_SERVERS_HOST,
    "acks": 1,                          # Chi can Leader ghi thanh cong la duoc
    "batch_size": 131072,               # 128 KB (batch lon hon cho luu luong clickstream cao)
    "linger_ms": 20,                    # Cho 20ms gom tin nhan
    "compression_type": "lz4",
}

