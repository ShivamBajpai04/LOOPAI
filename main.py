import asyncio
from typing import List
from uuid import UUID, uuid4
from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from models import (
    IngestionRequest,
    IngestionResponse,
    IngestionStatusResponse,
    IngestionJob,
    Batch,
    BatchStatus,
    Priority,
    BatchStatusResponse
)
from storage import storage
from scheduler import processor

@asynccontextmanager
async def lifespan(app: FastAPI):
    storage._lock = asyncio.Lock()
    await processor.start()
    yield
    await processor.stop()

app = FastAPI(title="Data Ingestion API", lifespan=lifespan)

def create_batches(ids: List[int], batch_size: int = 3) -> List[Batch]:
    """Split IDs into batches of specified size."""
    return [
        Batch(batch_id=uuid4(), ids=ids[i:i + batch_size])
        for i in range(0, len(ids), batch_size)
    ]

@app.post("/ingest", response_model=IngestionResponse)
async def ingest_data(request: IngestionRequest) -> IngestionResponse:
    """Create a new ingestion job and queue it for processing."""
    # Create ingestion job with batches
    ingestion_id = uuid4()
    batches = create_batches(request.ids)
    job = IngestionJob(
        ingestion_id=ingestion_id,
        priority=request.priority,
        batches=batches
    )

    # Store the job
    async with storage._lock:
        await storage.create_job(job)
        await processor.add_job(job)

    return IngestionResponse(ingestion_id=ingestion_id)

@app.get("/status/{ingestion_id}", response_model=IngestionStatusResponse)
async def get_status(ingestion_id: str) -> IngestionStatusResponse:
    """Get the current status of an ingestion job."""
    try:
        ingestion_id = UUID(ingestion_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Invalid ingestion ID format")

    job = await storage.get_job(ingestion_id)
    if not job:
        raise HTTPException(status_code=404, detail="Ingestion job not found")

    status = storage.get_job_status(job)
    return IngestionStatusResponse(
        ingestion_id=job.ingestion_id,
        status=status,
        batches=[
            BatchStatusResponse(
                batch_id=batch.batch_id,
                ids=batch.ids,
                status=batch.status
            )
            for batch in job.batches
        ]
    )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 