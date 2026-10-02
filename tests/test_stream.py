"""Stream bus: publish/consume roundtrip on the memory backend."""

from common.stream import MemoryStreamBus


def test_memory_bus_roundtrip():
    bus = MemoryStreamBus()
    bus.publish({"sensor_id": "NYC-001", "speed_mph": 42.0})
    bus.publish({"sensor_id": "NYC-002", "speed_mph": 55.0})
    batch = bus.read_batch(batch_size=10, block_ms=100)
    assert len(batch) == 2
    assert {m["sensor_id"] for m in batch} == {"NYC-001", "NYC-002"}
    assert bus.read_batch(batch_size=10, block_ms=50) == []
