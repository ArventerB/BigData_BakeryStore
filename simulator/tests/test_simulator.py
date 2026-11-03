import gzip
import json
import re
import sys
import types
from collections import Counter
from pathlib import Path

import kafka_config
import simulator


def test_file_mode_writes_dataset_and_manifest(tmp_path, monkeypatch):
    monkeypatch.setattr(simulator, "RESULTS_DIR", tmp_path / "results")
    out = tmp_path / "ds"
    simulator.main(["--mode", "file", "--start", "2026-10-02", "--days", "3", "--sessions-per-day", "300",
                    "--workers", "2", "--dirty-rate", "0.02", "--seed", "1", "--gzip", "--out", str(out)])
    manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    assert sorted(manifest["days"]) == ["2026-10-02", "2026-10-03", "2026-10-04"]
    total = 0
    for day, info in manifest["days"].items():
        with gzip.open(out / info["file"], "rt", encoding="utf-8") as f:
            lines = f.read().splitlines()
        assert len(lines) == info["events"]
        total += len(lines)
    assert total == manifest["events_total"]
    # thứ 7, chủ nhật đông hơn ngày thường
    days = manifest["days"]
    assert days["2026-10-03"]["sessions"] > days["2026-10-02"]["sessions"]
    assert list((tmp_path / "results").glob("sim_file_*.json"))


def test_file_mode_is_reproducible(tmp_path, monkeypatch):
    monkeypatch.setattr(simulator, "RESULTS_DIR", tmp_path / "results")
    args = ["--mode", "file", "--start", "2026-10-05", "--days", "1", "--sessions-per-day", "200",
            "--workers", "1", "--seed", "4"]
    simulator.main(args + ["--out", str(tmp_path / "a")])
    simulator.main(args + ["--out", str(tmp_path / "b")])
    a = (tmp_path / "a" / "date=2026-10-05" / "events.jsonl").read_bytes()
    b = (tmp_path / "b" / "date=2026-10-05" / "events.jsonl").read_bytes()
    assert a == b


def test_topic_routing_matches_tv1_topics():
    # tên topic phải trùng với scripts/create_topics.ps1 của TV1
    script = (Path(simulator.__file__).resolve().parent.parent / "scripts" / "create_topics.ps1").read_text(encoding="utf-8")
    created = set(re.findall(r'Create-Topic "([^"]+)"', script))
    routed = {simulator.topic_for({"event_type": t}) for t in
              ("page_view", "search", "product_view", "add_to_cart", "remove_from_cart", "checkout_start",
               "order_placed", "payment", "clik")}
    assert routed == {"bakery.user-events", "bakery.orders", "bakery.payments", "bakery.dlq"}
    assert routed <= created
    assert simulator.topic_for("không phải object") == "bakery.dlq"


def test_producer_config_follows_tv1():
    bootstrap, critical, fast = kafka_config.tv1_profiles()
    assert bootstrap == "localhost:9092,localhost:9094,localhost:9096"
    assert critical["acks"] == "all" and critical["enable.idempotence"] is True
    assert fast["acks"] == "1" and fast["enable.idempotence"] is False
    assert fast["batch.size"] == 131072 and critical["compression.type"] == "lz4"
    assert simulator.parse_args(["--mode", "kafka"]).bootstrap == bootstrap


def test_kafka_sender_uses_tv1_profiles_and_user_key(monkeypatch):
    sent, producers = [], []

    class FakeProducer:
        def __init__(self, conf):
            self.conf = conf
            producers.append(self)

        def produce(self, topic, key, value, on_delivery):
            sent.append((topic, key, self.conf["acks"]))

        def poll(self, timeout):
            return 0

        def flush(self, timeout):
            return 0

    monkeypatch.setitem(sys.modules, "confluent_kafka", types.SimpleNamespace(Producer=FakeProducer))
    args = simulator.parse_args(["--mode", "kafka"])
    sender = simulator.KafkaSender(args)
    sender.send({"event_type": "payment", "user_id": "U0000001"})
    sender.send({"event_type": "page_view", "user_id": "A00ff"})
    sender.send('{"event_type": "page_view"}')
    assert sent == [("bakery.payments", b"U0000001", "all"), ("bakery.user-events", b"A00ff", "1"),
                    ("bakery.dlq", b"unknown", "all")]
    # thí nghiệm: --acks ghi đè cho mọi topic, --topic chỉ dùng 1 producer
    producers.clear()
    sender = simulator.KafkaSender(simulator.parse_args(["--mode", "kafka", "--acks", "0", "--topic", "bakery.bench.p3"]))
    assert len(producers) == 1 and producers[0].conf["acks"] == "0"
    assert producers[0].conf["enable.idempotence"] is False


def test_stats_writer_merges_workers(tmp_path):
    path = tmp_path / "s.csv"
    w = simulator.StatsWriter(path, workers=2)
    tick = {"idx": 1, "t": 5.0, "window": 5, "sent": 500, "target": 100, "mult": 1, "hour": None,
            "errors": 0, "rejected": 3, "lat": [1.0, 2.0, 3.0]}
    w.add({**tick, "wid": 0})
    w.add({**tick, "wid": 1})
    w.add({**tick, "wid": 0, "idx": 2, "t": 7.5, "window": 2.5})  # chu kỳ cuối chỉ 1 worker kịp báo
    w.close()
    rows = path.read_text(encoding="utf-8").splitlines()
    assert len(rows) == 3
    assert rows[1].split(",")[5:9] == ["200.0", "1000", "0", "6"]
    assert rows[2].split(",")[5:7] == ["200.0", "1500"]
    assert Counter(r.count(",") for r in rows).most_common(1)[0][0] == len(simulator.StatsWriter.FIELDS) - 1
