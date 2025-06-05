import asyncio
import time
import pytest
from fastapi.testclient import TestClient
from uuid import UUID
from main import app
from models import Priority, BatchStatus
from storage import storage
from scheduler import processor

@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c

@pytest.fixture(autouse=True)
async def setup_and_teardown():
    """Setup and teardown for each test."""
    # Reset storage and processor
    storage._jobs.clear()
    # storage._lock = asyncio.Lock()  # No longer needed
    processor.queue._queue.clear()
    processor._last_batch_time = None
    
    # Start processor
    await processor.start()
    yield
    # Stop processor
    await processor.stop()

def test_ingest_endpoint(client):
    """Test the /ingest endpoint."""
    response = client.post(
        "/ingest",
        json={"ids": [1, 2, 3, 4, 5], "priority": "HIGH"}
    )
    assert response.status_code == 200
    data = response.json()
    assert "ingestion_id" in data
    assert UUID(data["ingestion_id"])  # Validates UUID format

def test_status_endpoint(client):
    """Test the /status endpoint."""
    # First create a job
    ingest_response = client.post(
        "/ingest",
        json={"ids": [1, 2, 3], "priority": "HIGH"}
    )
    ingestion_id = ingest_response.json()["ingestion_id"]

    # Then check its status
    response = client.get(f"/status/{ingestion_id}")
    assert response.status_code == 200
    data = response.json()
    assert data["ingestion_id"] == ingestion_id
    assert data["status"] in ["yet_to_start", "triggered", "completed"]
    assert len(data["batches"]) == 1  # Single batch of 3 IDs
    assert len(data["batches"][0]["ids"]) == 3

def test_invalid_ingestion_id(client):
    """Test status endpoint with invalid ingestion ID."""
    response = client.get("/status/invalid-uuid")
    assert response.status_code == 404

@pytest.mark.asyncio
async def test_priority_ordering(client):
    """Test that jobs are processed in priority order."""
    # Submit jobs in reverse priority order
    low_job = client.post(
        "/ingest",
        json={"ids": [1, 2, 3], "priority": "LOW"}
    ).json()
    
    medium_job = client.post(
        "/ingest",
        json={"ids": [4, 5, 6], "priority": "MEDIUM"}
    ).json()
    
    high_job = client.post(
        "/ingest",
        json={"ids": [7, 8, 9], "priority": "HIGH"}
    ).json()

    # Process batches explicitly
    # Process HIGH priority batch
    assert await processor.process_next_batch()  # Should process HIGH priority batch
    time.sleep(1)  # Small delay to allow status updates

    # Check statuses - HIGH should be triggered/completed
    high_status = client.get(f"/status/{high_job['ingestion_id']}").json()
    medium_status = client.get(f"/status/{medium_job['ingestion_id']}").json()
    low_status = client.get(f"/status/{low_job['ingestion_id']}").json()

    # HIGH priority job should be more advanced in processing
    assert high_status["status"] in ["triggered", "completed"]
    # MEDIUM and LOW should still be yet_to_start
    assert medium_status["status"] == "yet_to_start"
    assert low_status["status"] == "yet_to_start"

def test_batch_size_limit(client):
    """Test that batches are limited to 3 IDs."""
    response = client.post(
        "/ingest",
        json={"ids": list(range(10)), "priority": "HIGH"}
    )
    assert response.status_code == 200
    ingestion_id = response.json()["ingestion_id"]

    status = client.get(f"/status/{ingestion_id}").json()
    # Should have 4 batches: 3 + 3 + 3 + 1
    assert len(status["batches"]) == 4
    # All batches except last should have 3 IDs
    for batch in status["batches"][:-1]:
        assert len(batch["ids"]) == 3
    # Last batch should have 1 ID
    assert len(status["batches"][-1]["ids"]) == 1

@pytest.mark.asyncio
async def test_rate_limiting(client):
    """Test that batches are processed with 5-second intervals."""
    # Submit two jobs
    job1 = client.post(
        "/ingest",
        json={"ids": [1, 2, 3], "priority": "HIGH"}
    ).json()
    
    job2 = client.post(
        "/ingest",
        json={"ids": [4, 5, 6], "priority": "HIGH"}
    ).json()

    # Process first batch explicitly
    assert await processor.process_next_batch()  # Should process first batch
    time.sleep(1)  # Small delay to allow status updates

    # Check statuses
    status1 = client.get(f"/status/{job1['ingestion_id']}").json()
    status2 = client.get(f"/status/{job2['ingestion_id']}").json()

    # First job should be triggered/completed
    assert status1["status"] in ["triggered", "completed"]
    # Second job should still be yet_to_start due to rate limiting
    assert status2["status"] == "yet_to_start"

    # Try to process second batch immediately - should fail due to rate limiting
    assert not await processor.process_next_batch()  # Should not process due to rate limit

def test_concurrent_requests(client):
    """Test handling of concurrent ingestion requests."""
    # Submit multiple jobs simultaneously
    jobs = []
    for i in range(5):
        response = client.post(
            "/ingest",
            json={"ids": [i*3 + 1, i*3 + 2, i*3 + 3], "priority": "HIGH"}
        )
        assert response.status_code == 200
        jobs.append(response.json()["ingestion_id"])

    # Wait a bit for processing
    time.sleep(6)

    # Check all jobs were created successfully
    for job_id in jobs:
        response = client.get(f"/status/{job_id}")
        assert response.status_code == 200
        data = response.json()
        assert data["ingestion_id"] == job_id
        assert data["status"] in ["yet_to_start", "triggered", "completed"] 