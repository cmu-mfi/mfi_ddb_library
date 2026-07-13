from pathlib import Path
import sys

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dws.piwebapi import PIWebAPI


@pytest.fixture(scope="module")
def config():
    secrets_path = ROOT / "dws" / "config.yaml"
    with open(secrets_path, "r") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def pi(config):
    return PIWebAPI(config)


TEST_TOPICS = [
    # (
    #     r"mfi-v1.0-historian/Mill-19/Server-Room/telemetry/DATA/data/temperature",
    #     "2026-03-17 00:00:00",
    #     "2026-03-17 00:00:00",
    #     "2026-03-17 01:00:00",
    # ),
    (
        r"mfi-v1.0-historian/Mill-19/AV-Room/telemetry/#",
        "2026-03-17 00:00:00",
        "2025-10-21 10:00:00",
        "2025-10-21 10:01:00",
    ),
    # (
    #     r"bad/topic",
    #     "2026-03-17 00:00:00",
    #     "2026-03-17 00:00:00",
    #     "2026-03-17 01:00:00",
    # ),
]


def test_connection(pi):
    url = f"{pi.url}/dataservers"
    response = pi.session.get(url, timeout=30)
    assert response.status_code == 200, f"Failed with {response.status_code}: {response.text}"
    data = response.json()
    assert "Items" in data
    print(f"Successfully connected to PI Web API. Found {len(data['Items'])} dataservers.")


@pytest.mark.parametrize("topic,timestamp,start_time,end_time", TEST_TOPICS)
def test_get_webid(pi, topic, timestamp, start_time, end_time):
    try:
        result = pi._PIWebAPI__get_topic_webid(topic)
        print(f"\nWebId map returned: {result}")
    except Exception as e:
        pytest.fail(f"WebId lookup failed: {e}")
    assert isinstance(result, dict)


@pytest.mark.parametrize("topic,timestamp,start_time,end_time", TEST_TOPICS)
def test_get_data_point(pi, topic, timestamp, start_time, end_time):
    result = pi.get_data_point(topic=topic, timestamp=timestamp)
    print(f"\nData point returned: {result}")
    if result is not None:
        assert "timestamp" in result
        assert "topic" in result
        assert "value" in result


@pytest.mark.parametrize("topic,timestamp,start_time,end_time", TEST_TOPICS)
def test_get_data_range(pi, topic, timestamp, start_time, end_time):
    print(f"\nTesting GetDataRange with topic='{topic}', start_time='{start_time}', end_time='{end_time}'")
    result = pi.get_data_range(
        topic=topic,
        start_time=start_time,
        end_time=end_time,
        page_size=10,
        page_token=None
    )
    print(f"\nData range returned: {result}")
    assert "data" in result
    assert "nextPageToken" in result
    assert len(result["data"]) <= 10
    for item in result["data"]:
        assert "timestamp" in item
        assert "topic" in item
        assert "value" in item


@pytest.mark.parametrize("topic,timestamp,start_time,end_time", TEST_TOPICS)
def test_get_data_range_all_pages(pi, topic, timestamp, start_time, end_time):
    page_token = None
    all_data = []
    while True:
        result = pi.get_data_range(
            topic=topic,
            start_time=start_time,
            end_time=end_time,
            page_size=1000,
            page_token=page_token
        )
        assert "data" in result
        assert "nextPageToken" in result
        all_data.extend(result["data"])
        if result["nextPageToken"] is None:
            break
        page_token = result["nextPageToken"]
        print(f'{result["topic"]} - Fetched {len(result["data"])} items, nextPageToken={page_token}')
    print(f"Fetched {len(result['data'])} items")
    # print(f"All data: {all_data}")

    assert len(all_data) > 0
    for item in all_data:
        assert "timestamp" in item
        assert "topic" in item
        assert "value" in item
    timestamps = [item["timestamp"] for item in all_data]
    assert timestamps == sorted(timestamps)