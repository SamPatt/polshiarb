from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from app.db import DEFAULT_DB_PATH, init_db
from app.url_normalization import URLNormalizationError, normalize_pair_urls

app = FastAPI(title="Polshiarb Pair Manager")
templates = Jinja2Templates(directory="templates")


class NormalizePairRequest(BaseModel):
    kalshi_url: str
    polymarket_url: str


@app.on_event("startup")
def startup() -> None:
    init_db(DEFAULT_DB_PATH)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok", "db_path": str(DEFAULT_DB_PATH)}


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "page_title": "Pair Manager"},
    )


@app.post("/api/normalize-pair")
def normalize_pair(payload: NormalizePairRequest) -> dict[str, object]:
    try:
        normalized = normalize_pair_urls(
            kalshi_url=payload.kalshi_url,
            polymarket_url=payload.polymarket_url,
        )
        return {"ok": True, "normalized": normalized}
    except URLNormalizationError as exc:
        return {"ok": False, "error": str(exc)}
