from typing import Dict, Optional
from uuid import UUID
from models import IngestionJob, BatchStatus

class Storage:
    def __init__(self):
        self._jobs: Dict[UUID, IngestionJob] = {}
        self._lock = None  # Will be set to asyncio.Lock in main.py

    async def create_job(self, job: IngestionJob) -> None:
        """Store a new ingestion job."""
        self._jobs[job.ingestion_id] = job

    async def get_job(self, ingestion_id: UUID) -> Optional[IngestionJob]:
        """Retrieve an ingestion job by ID."""
        return self._jobs.get(ingestion_id)

    async def update_batch_status(self, ingestion_id: UUID, batch_id: UUID, status: BatchStatus) -> bool:
        """Update the status of a specific batch within a job."""
        job = self._jobs.get(ingestion_id)
        if not job:
            return False

        for batch in job.batches:
            if batch.batch_id == batch_id:
                batch.status = status
                return True
        return False

    def get_job_status(self, job: IngestionJob) -> BatchStatus:
        """Calculate the overall status of a job based on its batches."""
        if not job.batches:
            return BatchStatus.YET_TO_START

        statuses = {batch.status for batch in job.batches}
        
        if BatchStatus.YET_TO_START in statuses and len(statuses) == 1:
            return BatchStatus.YET_TO_START
        elif BatchStatus.COMPLETED in statuses and len(statuses) == 1:
            return BatchStatus.COMPLETED
        else:
            return BatchStatus.TRIGGERED

# Global storage instance
storage = Storage() 