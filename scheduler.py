import asyncio
from typing import List, Tuple, Optional
from uuid import UUID, uuid4
from datetime import datetime
from models import Priority, Batch, BatchStatus, IngestionJob
from storage import storage

class PriorityQueue:
    def __init__(self):
        self._queue: List[Tuple[float, datetime, Batch]] = []
        self._lock = asyncio.Lock()

    async def put(self, batch: Batch, priority: Priority) -> None:
        """Add a batch to the queue with its priority."""
        async with self._lock:
            # Priority weight (1=HIGH, 2=MEDIUM, 3=LOW)
            weight = {"HIGH": 1, "MEDIUM": 2, "LOW": 3}[priority]
            self._queue.append((weight, datetime.utcnow(), batch))
            self._queue.sort(key=lambda x: (x[0], x[1]))

    async def get(self) -> Optional[Batch]:
        """Get the highest priority batch from the queue."""
        async with self._lock:
            if not self._queue:
                return None
            return self._queue.pop(0)[2]

    def is_empty(self) -> bool:
        """Check if the queue is empty."""
        return len(self._queue) == 0

class BatchProcessor:
    def __init__(self):
        self.queue = PriorityQueue()
        self._running = False
        self._last_batch_time = datetime.min
        self._rate_limit_lock = asyncio.Lock()

    async def start(self):
        """Start the batch processor."""
        self._running = True
        asyncio.create_task(self._process_loop())

    async def stop(self):
        """Stop the batch processor."""
        self._running = False

    async def add_job(self, job: IngestionJob) -> None:
        """Add all batches from a job to the queue."""
        for batch in job.batches:
            await self.queue.put(batch, job.priority)

    async def process_next_batch(self) -> bool:
        """Process the next batch in the queue if available.
        Returns True if a batch was processed, False otherwise."""
        if self.queue.is_empty():
            return False

        # Rate limiting: ensure 5 seconds between batches
        async with self._rate_limit_lock:
            now = datetime.utcnow()
            time_since_last = (now - self._last_batch_time).total_seconds()
            if time_since_last < 5:
                await asyncio.sleep(5 - time_since_last)
            
            batch = await self.queue.get()
            if batch:
                await self._process_batch(batch)
                self._last_batch_time = datetime.utcnow()
                return True
        return False

    async def _process_loop(self):
        """Main processing loop that handles batches."""
        while self._running:
            if self.queue.is_empty():
                await asyncio.sleep(1)
                continue

            await self.process_next_batch()

    async def _process_batch(self, batch: Batch) -> None:
        """Process a single batch of IDs."""
        # Update batch status to triggered
        await storage.update_batch_status(batch.batch_id, batch.batch_id, BatchStatus.TRIGGERED)

        # Simulate processing each ID
        for id in batch.ids:
            # Simulate API call
            await asyncio.sleep(1)
            # In a real system, you would make an actual API call here
            # response = await api_client.process(id)

        # Update batch status to completed
        await storage.update_batch_status(batch.batch_id, batch.batch_id, BatchStatus.COMPLETED)

# Global processor instance
processor = BatchProcessor() 