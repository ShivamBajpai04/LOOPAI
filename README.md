# Data Ingestion API System

A FastAPI-based system for handling asynchronous data ingestion with priority queuing and rate limiting.

## Features

- RESTful API endpoints for job submission and status tracking
- Priority-based job queuing (HIGH, MEDIUM, LOW)
- Automatic batch processing (max 3 IDs per batch)
- Rate limiting (1 batch per 5 seconds)
- Asynchronous processing with status tracking
- In-memory storage with thread-safe operations

## Setup

1. Create a virtual environment (recommended):

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run the server:

```bash
uvicorn main:app --reload
```

The API will be available at `http://localhost:8000`

## API Endpoints

### POST /ingest

Submit a new ingestion job.

Request:

```json
{
  "ids": [1, 2, 3, 4, 5],
  "priority": "HIGH"
}
```

Response:

```json
{
  "ingestion_id": "uuid-here"
}
```

### GET /status/{ingestion_id}

Get the status of an ingestion job.

Response:

```json
{
  "ingestion_id": "uuid-here",
  "status": "triggered",
  "batches": [
    {
      "batch_id": "uuid-here",
      "ids": [1, 2, 3],
      "status": "completed"
    },
    {
      "batch_id": "uuid-here",
      "ids": [4, 5],
      "status": "triggered"
    }
  ]
}
```

## Priority Levels

- HIGH (weight: 1)
- MEDIUM (weight: 2)
- LOW (weight: 3)

Jobs are processed in order of priority (lowest weight first) and then by creation time.

## Status Values

- yet_to_start: Job is queued but not yet processed
- triggered: Job is currently being processed
- completed: Job has finished processing

## Development

The project structure:

```
ingestion-api/
├── main.py          # FastAPI application and endpoints
├── models.py        # Data models and schemas
├── scheduler.py     # Priority queue and batch processing
├── storage.py       # In-memory storage
├── requirements.txt # Project dependencies
└── README.md       # This file
```

## Testing

Run the test suite:

```bash
pytest test_ingestion.py
```
