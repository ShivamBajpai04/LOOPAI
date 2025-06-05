from enum import Enum
from typing import List, Optional
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel, Field

class Priority(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

class BatchStatus(str, Enum):
    YET_TO_START = "yet_to_start"
    TRIGGERED = "triggered"
    COMPLETED = "completed"

class Batch(BaseModel):
    batch_id: UUID
    ids: List[int]
    status: BatchStatus = BatchStatus.YET_TO_START

    class Config:
        orm_mode = True

class IngestionJob(BaseModel):
    ingestion_id: UUID
    priority: Priority
    created_at: datetime = Field(default_factory=datetime.utcnow)
    batches: List[Batch]

    class Config:
        orm_mode = True

class IngestionRequest(BaseModel):
    ids: List[int]
    priority: Priority = Priority.MEDIUM

    class Config:
        orm_mode = True

class IngestionResponse(BaseModel):
    ingestion_id: UUID

    class Config:
        orm_mode = True

class BatchStatusResponse(BaseModel):
    batch_id: UUID
    ids: List[int]
    status: BatchStatus

    class Config:
        orm_mode = True

class IngestionStatusResponse(BaseModel):
    ingestion_id: UUID
    status: BatchStatus
    batches: List[BatchStatusResponse]

    class Config:
        orm_mode = True 