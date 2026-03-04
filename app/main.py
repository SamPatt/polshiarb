from __future__ import annotations

from functools import lru_cache
from typing import Any, Literal

from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from app.db import (
    DEFAULT_DB_PATH,
    delete_pair_set,
    init_db,
    list_pair_sets,
    list_monitoring_links,
    load_pair_set,
    save_pair_set,
    update_pair_set,
)
from app.pmxt_adapter import PMXTAdapter, PMXTAdapterError
from app.url_normalization import URLNormalizationError, normalize_pair_urls

app = FastAPI(title="Polshiarb Pair Manager")
templates = Jinja2Templates(directory="templates")


class NormalizePairRequest(BaseModel):
    kalshi_url: str
    polymarket_url: str


class PreviewPairRequest(BaseModel):
    kalshi_url: str
    polymarket_url: str


class PairMatchSelection(BaseModel):
    kalshi_market_id: str
    polymarket_market_id: str
    relation_type: str = "same_direction"
    active: bool = True


class SavePairRequest(BaseModel):
    kalshi_url: str
    polymarket_url: str
    normalized: dict[str, Any]
    preview: dict[str, Any]
    matches: list[PairMatchSelection] = Field(default_factory=list)
    recurrence_intent: str | None = None
    expires_at: str | None = None


class PairListResponse(BaseModel):
    ok: bool
    pairs: list[dict[str, Any]] = Field(default_factory=list)
    status: str = "all"


class MonitoringLeg(BaseModel):
    exchange: Literal["kalshi", "polymarket"]
    market_id: str
    outcome_id: str | None = None


class MonitoringMapping(BaseModel):
    relation_type: str
    legs: list[MonitoringLeg]


class MonitoringPair(BaseModel):
    pair_id: int
    recurrence_intent: str | None = None
    expires_at: str | None = None
    mappings: list[MonitoringMapping] = Field(default_factory=list)


class MonitoringPairsResponse(BaseModel):
    ok: bool
    pairs: list[MonitoringPair] = Field(default_factory=list)
    active_only: bool = True
    include_expired: bool = False


@lru_cache(maxsize=1)
def get_pmxt_adapter() -> PMXTAdapter:
    return PMXTAdapter()


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
        {
            "request": request,
            "page_title": "Pair Manager",
            "is_edit_mode": False,
            "edit_pair_id": None,
        },
    )


@app.get("/pairs", response_class=HTMLResponse)
def pairs_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "pairs.html",
        {
            "request": request,
            "page_title": "Saved Pairs",
        },
    )


@app.get("/pairs/{pair_id}/edit", response_class=HTMLResponse)
def edit_pair_page(request: Request, pair_id: int) -> HTMLResponse:
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "page_title": f"Edit Pair #{pair_id}",
            "is_edit_mode": True,
            "edit_pair_id": pair_id,
        },
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


@app.post("/api/preview")
def preview_pair(payload: PreviewPairRequest) -> dict[str, object]:
    try:
        normalized = normalize_pair_urls(
            kalshi_url=payload.kalshi_url,
            polymarket_url=payload.polymarket_url,
        )
    except URLNormalizationError as exc:
        return {"ok": False, "error": str(exc)}

    try:
        preview = get_pmxt_adapter().preview_from_normalized(normalized)
    except PMXTAdapterError as exc:
        return {"ok": False, "error": str(exc), "normalized": normalized}

    return {"ok": True, "normalized": normalized, "preview": preview}


@app.post("/api/pairs")
def create_pair(payload: SavePairRequest) -> dict[str, object]:
    try:
        save_result = save_pair_set(
            db_path=DEFAULT_DB_PATH,
            payload={
                "kalshi_url": payload.kalshi_url,
                "polymarket_url": payload.polymarket_url,
                "normalized": payload.normalized,
                "preview": payload.preview,
                "matches": [match.model_dump() for match in payload.matches],
                "recurrence_intent": payload.recurrence_intent,
                "expires_at": payload.expires_at,
            },
        )
    except Exception as exc:
        return {"ok": False, "error": f"Failed to save pair: {exc}"}

    return {"ok": True, **save_result}


@app.get("/api/pairs")
def list_pairs(status: str = Query(default="all")) -> PairListResponse:
    if status not in {"all", "active", "expired"}:
        return PairListResponse(ok=False, status=status)
    pairs = list_pair_sets(DEFAULT_DB_PATH, status=status)
    return PairListResponse(ok=True, pairs=pairs, status=status)


@app.get("/api/pairs/{pair_id}")
def get_pair(pair_id: int) -> dict[str, object]:
    pair = load_pair_set(DEFAULT_DB_PATH, pair_id)
    if pair is None:
        return {"ok": False, "error": f"Pair {pair_id} not found"}
    return {"ok": True, "pair": pair}


@app.put("/api/pairs/{pair_id}")
def put_pair(pair_id: int, payload: SavePairRequest) -> dict[str, object]:
    try:
        update_result = update_pair_set(
            db_path=DEFAULT_DB_PATH,
            pair_id=pair_id,
            payload={
                "kalshi_url": payload.kalshi_url,
                "polymarket_url": payload.polymarket_url,
                "normalized": payload.normalized,
                "preview": payload.preview,
                "matches": [match.model_dump() for match in payload.matches],
                "recurrence_intent": payload.recurrence_intent,
                "expires_at": payload.expires_at,
            },
        )
    except Exception as exc:
        return {"ok": False, "error": f"Failed to update pair: {exc}"}

    if update_result is None:
        return {"ok": False, "error": f"Pair {pair_id} not found"}
    return {"ok": True, **update_result}


@app.delete("/api/pairs/{pair_id}")
def remove_pair(pair_id: int) -> dict[str, object]:
    deleted = delete_pair_set(DEFAULT_DB_PATH, pair_id)
    if not deleted:
        return {"ok": False, "error": f"Pair {pair_id} not found"}
    return {"ok": True, "pair_id": pair_id}


@app.get("/api/monitoring/pairs")
def get_monitoring_pairs(
    active_only: bool = Query(default=True),
    include_expired: bool = Query(default=False),
) -> MonitoringPairsResponse:
    links = list_monitoring_links(
        db_path=DEFAULT_DB_PATH,
        active_only=active_only,
        include_expired=include_expired,
    )

    grouped_pairs: dict[int, MonitoringPair] = {}
    for link in links:
        pair_id = int(link["pair_id"])
        pair = grouped_pairs.get(pair_id)
        if pair is None:
            pair = MonitoringPair(
                pair_id=pair_id,
                recurrence_intent=link.get("recurrence_intent"),
                expires_at=link.get("expires_at"),
                mappings=[],
            )
            grouped_pairs[pair_id] = pair

        pair.mappings.append(
            MonitoringMapping(
                relation_type=str(link.get("relation_type") or "same_direction"),
                legs=[
                    MonitoringLeg(
                        exchange="kalshi",
                        market_id=str(link["kalshi_market_id"]),
                        outcome_id=(
                            str(link["kalshi_outcome_id"])
                            if link.get("kalshi_outcome_id") is not None
                            else None
                        ),
                    ),
                    MonitoringLeg(
                        exchange="polymarket",
                        market_id=str(link["polymarket_market_id"]),
                        outcome_id=(
                            str(link["polymarket_outcome_id"])
                            if link.get("polymarket_outcome_id") is not None
                            else None
                        ),
                    ),
                ],
            )
        )

    ordered_pairs = sorted(grouped_pairs.values(), key=lambda item: item.pair_id, reverse=True)
    return MonitoringPairsResponse(
        ok=True,
        pairs=ordered_pairs,
        active_only=active_only,
        include_expired=include_expired,
    )
