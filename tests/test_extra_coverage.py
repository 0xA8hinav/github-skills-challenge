import os
import sys
import runpy
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir)))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, "src")))
import pytest
from src.calculations import area_of_circle, get_nth_fibonacci
from src.anomaly_detector import AnomalyDetector
from src.event_topic import EventTopic
from src.event_producer import EventProducer
from src.event_consumer import EventConsumer
from src.aiops_pipeline import load_data, run_pipeline


def test_area_of_circle_negative_radius():
    with pytest.raises(ValueError):
        area_of_circle(-1)


def test_get_nth_fibonacci_negative():
    with pytest.raises(ValueError):
        get_nth_fibonacci(-1)


def test_get_nth_fibonacci_ten():
    assert get_nth_fibonacci(10) == 55


def test_get_nth_fibonacci_two():
    assert get_nth_fibonacci(2) == 1


def test_high_cpu_detected():
    detector = AnomalyDetector()
    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 100,
        "cpu_percent": 95,
        "memory_percent": 50,
        "log_level": "INFO",
        "message": "ok"
    }
    event = detector.detect(record)
    assert event is not None
    assert "High CPU utilization" in event["reasons"]


def test_high_memory_detected():
    detector = AnomalyDetector()
    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 100,
        "cpu_percent": 40,
        "memory_percent": 95,
        "log_level": "INFO",
        "message": "ok"
    }
    event = detector.detect(record)
    assert event is not None
    assert "High memory utilization" in event["reasons"]


def test_warning_log_detected():
    detector = AnomalyDetector()
    record = {
        "timestamp": "2026-09-20T10:00:00",
        "service": "payment-service",
        "response_time_ms": 100,
        "cpu_percent": 40,
        "memory_percent": 50,
        "log_level": "WARNING",
        "message": "warn"
    }
    event = detector.detect(record)
    assert event is not None
    assert "Error log detected" in event["reasons"]


def test_producer_rejects_empty_event():
    topic = EventTopic("t")
    producer = EventProducer(topic)
    assert producer.publish(None) is False
    assert producer.publish({}) is False


def test_topic_clear():
    topic = EventTopic("t")
    topic.publish({"a": 1})
    assert len(topic.get_messages()) == 1
    topic.clear()
    assert topic.get_messages() == []


def test_consumer_empty():
    topic = EventTopic("t")
    consumer = EventConsumer(topic)
    assert consumer.consume() == []


def test_load_data():
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, "data", "service_data.json"))
    data = load_data(path)
    assert len(data) == 10


def test_run_pipeline():
    path = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, "data", "service_data.json"))
    result = run_pipeline(path)
    assert result["records_processed"] == 10
    assert len(result["anomalies_detected"]) >= 1
    assert "events_consumed" in result


def test_pipeline_main():
    cwd = os.getcwd()
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    os.chdir(root)
    try:
        runpy.run_path(os.path.join(root, "src", "aiops_pipeline.py"), run_name="__main__")
    finally:
        os.chdir(cwd)
