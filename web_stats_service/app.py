import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from .db import StatsDB


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
db_override = str(os.getenv("SWISS_STATS_DB", "")).strip()
if db_override:
    DB_PATH = Path(db_override)
elif getattr(sys, "frozen", False):
    DB_PATH = Path.cwd() / "web_stats_results.db"
else:
    DB_PATH = BASE_DIR / "results.db"

app = FastAPI(title="SwissStats Service", version="1.0.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db = StatsDB(DB_PATH)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

_change_lock = threading.Lock()
_change_version = 0


def _bump_change() -> int:
    global _change_version
    with _change_lock:
        _change_version += 1
        return _change_version


def _get_change() -> int:
    with _change_lock:
        return _change_version


class IngestResult(BaseModel):
    event_id: str
    source: str = "quantum-server"
    created_at: Optional[str] = None
    category: str
    run_key: str
    run_started_at_text: str = ""
    distance_m: int
    bib: str
    name: str
    country: str = ""
    finish_sec: Optional[float] = None
    finish_text: str = ""
    status: str = ""
    splits: Dict[str, Any] = Field(default_factory=dict)


@app.get("/")
def index():
    return FileResponse(str(STATIC_DIR / "index.html"))


@app.post("/ingest/result")
def ingest_result(payload: IngestResult):
    p = payload.model_dump()
    if not p.get("created_at"):
        p["created_at"] = datetime.now().isoformat(timespec="seconds")
    ok = db.upsert_result(p)
    if ok:
        _bump_change()
    return {"ok": bool(ok)}


@app.get("/api/stream")
def api_stream():
    def event_gen():
        last = -1
        yield "retry: 1500\n\n"
        while True:
            cur = _get_change()
            if cur != last:
                last = cur
                yield f"event: change\ndata: {cur}\n\n"
            else:
                yield "event: ping\ndata: ok\n\n"
            time.sleep(1.0)

    return StreamingResponse(
        event_gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/api/categories")
def api_categories():
    return {"items": db.categories()}


@app.get("/api/athletes")
def api_athletes(category: str = Query(..., min_length=1)):
    return {"items": db.athletes(category)}


@app.get("/api/category/top")
def api_category_top(category: str = Query(..., min_length=1), limit: int = Query(30, ge=1, le=200)):
    return {"items": db.top_athletes_in_category(category, limit=limit)}


@app.get("/api/athlete/{athlete_id}/distances")
def api_distances(athlete_id: int):
    return {"items": db.distances(int(athlete_id))}


@app.get("/api/athlete/{athlete_id}/distance/{distance_m}/runs")
def api_runs(athlete_id: int, distance_m: int):
    return {"items": db.runs_for_distance(int(athlete_id), int(distance_m))}


@app.get("/api/athlete/{athlete_id}/best")
def api_best(athlete_id: int, distance_m: int = Query(..., ge=1)):
    row = db.best_for_distance(int(athlete_id), int(distance_m))
    if not row:
        return {"item": None}
    return {"item": row}


@app.get("/api/athlete/{athlete_id}/full")
def api_athlete_full(athlete_id: int):
    return {"items": db.athlete_full_profile(int(athlete_id))}


@app.get("/health")
def health():
    return {"ok": True}
