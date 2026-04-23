import logging
import os
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from src.pipeline import run_pipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()


class RunRequest(BaseModel):
    export_name: str | None = None
    partition: str | None = None  # YYYY-MM, e.g. "2026-04"


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/run")
def run(body: RunRequest = RunRequest()):
    try:
        result = run_pipeline(export_name=body.export_name, partition=body.partition)
        logger.info("Pipeline complete: %s", result)
        return result
    except Exception as exc:
        logger.exception("Pipeline failed")
        return JSONResponse(status_code=500, content={"error": str(exc)})


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
