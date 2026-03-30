from pathlib import Path
import sys
import pytest
import yaml

MODULE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_DIR))

from piwebapi import PIWebAPI


@pytest.fixture(scope="module")
def config():
    secrets_path = MODULE_DIR / "secrets.yaml"
    with open(secrets_path, "r") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def pi(config):
    return PIWebAPI(config)


def test_connection(pi):
    """
    Basic smoke test:
    - checks if API is reachable
    - checks if auth works
    """
    url = f"{pi.url}/dataservers"
    response = pi.session.get(url, timeout=30)

    assert response.status_code == 200, f"Failed with {response.status_code}: {response.text}"
    data = response.json()

    assert "Items" in data, "Response does not contain expected 'Items' field"
    print(f"Successfully connected to PI Web API. Found {len(data['Items'])} dataservers.")


def test_get_webid(pi, config):
    """
    Assuming the metadata store returns the correct topic for Aveva PI that includes /DATA or /ATTR
    """
    TEST_TOPIC = r"mfi-v1.0-historian/Mill-19/Server-Room/telemetry/DATA/data/temperature"  

    try:
        webid = pi._PIWebAPI__get_topic_webid(TEST_TOPIC)
    except Exception as e:
        pytest.fail(f"WebId lookup failed: {e}")

    assert webid is not None
    assert isinstance(webid, str)


def test_get_data_point(pi):
    """
    Test get_data_point, assusming the topic is given correctly by the metadata store
    """
    TEST_TOPIC = r"mfi-v1.0-historian/Mill-19/Server-Room/telemetry/DATA/data/temperature"
    TEST_TIMESTAMP = "2026-03-17 00:00:00"

    value = pi.get_data_point(
        topic=TEST_TOPIC,
        user_id=None,
        timestamp=TEST_TIMESTAMP
    )
    assert value is not None or value is None  # ensures no exception

def test_get_data_range(pi):
        """
            Test get_data_range, assuming the topic is given correctly by the metadata store first 10 values
        """
        TEST_TOPIC = r"mfi-v1.0-historian/Mill-19/Server-Room/telemetry/DATA/data/temperature"
        TEST_START_TIME = "2026-03-17 00:00:00"
        TEST_END_TIME = "2026-03-17 01:00:00"
        TEST_PAGE_SIZE = 10

        value = pi.get_data_range(
            topic=TEST_TOPIC,
            user_id=None,
            start_time=TEST_START_TIME,
            end_time=TEST_END_TIME,
            page_size=TEST_PAGE_SIZE,
            page_token=None
        )
        assert "data" in value
        assert "nextPageToken" in value
        assert len(value["data"]) <= TEST_PAGE_SIZE

def test_get_data_range_all_pages(pi):
    TEST_TOPIC = r"mfi-v1.0-historian/Mill-19/Server-Room/telemetry/DATA/data/temperature"
    TEST_START_TIME = "2026-03-17 00:00:00"
    TEST_END_TIME = "2026-03-17 01:00:00"
    TEST_PAGE_SIZE = 1000

    page_token = None
    all_data = []

    while True:
        result = pi.get_data_range(
            topic=TEST_TOPIC,
            user_id=None,
            start_time=TEST_START_TIME,
            end_time=TEST_END_TIME,
            page_size=TEST_PAGE_SIZE,
            page_token=page_token
        )

        assert "data" in result
        assert "nextPageToken" in result
        all_data.extend(result["data"])

        if result["nextPageToken"] is None:
            break

        page_token = result["nextPageToken"]

    assert len(all_data) > 0