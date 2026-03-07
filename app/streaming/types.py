from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol


StreamKey = tuple[str, str]
PrintFn = Callable[[str], None]
DebugFn = Callable[[str], None]
ExchangeBuilder = Callable[..., Any]


@dataclass(frozen=True)
class HealthState:
    active_streams: list[StreamKey]
    quote_count: int
    stale_quote_count: int
    client_exchanges: list[str]
    exchange_subscription_counts: dict[str, int]
    exchange_active_counts: dict[str, int]
    exchange_pending_counts: dict[str, int]
    exchange_priority_counts: dict[str, int]
    exchange_queue_depth_counts: dict[str, int]
    exchange_dispatch_drop_counts: dict[str, int]
    exchange_source_modes: dict[str, str]
    exchange_modes: dict[str, str]
    exchange_recovery_reasons: dict[str, str]
    retry_windows: dict[str, float]
    backoffs: dict[str, float]
    rate_limit_counts: dict[str, int]
    sidecar_error_counts: dict[str, int]
    auth_error_counts: dict[str, int]
    success_counts: dict[StreamKey, int]
    error_counts: dict[StreamKey, int]
    latencies_ms: dict[StreamKey, float]
    last_ok_at: dict[StreamKey, float]


class ExchangeIngestor(Protocol):
    stream_key: StreamKey

    def start(self) -> None: ...

    def stop(self) -> None: ...

    def join(self, timeout_seconds: float) -> bool: ...

    def is_alive(self) -> bool: ...


class SubscriptionManager(Protocol):
    def prepare_runtime(self) -> None: ...

    def replace_streams(self, stream_keys: set[StreamKey]) -> bool: ...

    def health_state(self, *, now: float) -> HealthState: ...

    def stop(self, *, reason: str, timeout_seconds: float) -> None: ...
