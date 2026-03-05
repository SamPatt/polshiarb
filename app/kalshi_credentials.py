from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping


_REPO_ROOT = Path(__file__).resolve().parent.parent


def _looks_like_pem(value: str) -> bool:
    return "BEGIN " in value and "PRIVATE KEY" in value


def resolve_kalshi_private_key(value: str | None) -> str | None:
    """Resolve KALSHI_PRIVATE_KEY value to PEM contents when a file path is provided."""
    if value is None:
        return None

    raw = value.strip()
    if not raw:
        return None
    if _looks_like_pem(raw):
        return raw

    candidate = Path(raw).expanduser()
    candidates = [candidate]
    if not candidate.is_absolute():
        candidates.append(_REPO_ROOT / candidate)

    for path in candidates:
        if path.is_file():
            try:
                return path.read_text(encoding="utf-8")
            except OSError:
                # Fall back to raw value to preserve PMXT default behavior.
                return raw

    return raw


def _parse_dotenv(path: Path) -> dict[str, str]:
    parsed: dict[str, str] = {}
    if not path.is_file():
        return parsed

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped[len("export ") :].strip()
        if "=" not in stripped:
            continue

        key, value = stripped.split("=", maxsplit=1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        parsed[key] = value
    return parsed


def _env_with_dotenv_fallback() -> dict[str, str]:
    merged: dict[str, str] = {}
    seen: set[Path] = set()
    for candidate in (Path.cwd() / ".env", _REPO_ROOT / ".env"):
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        merged.update(_parse_dotenv(resolved))
    merged.update(os.environ)
    return merged


def build_kalshi_client_kwargs(env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Build pmxt.Kalshi kwargs from environment, supporting path-valued private keys."""
    source: Mapping[str, str] = env if env is not None else _env_with_dotenv_fallback()
    kwargs: dict[str, str] = {}

    api_key = source.get("KALSHI_API_KEY")
    if api_key:
        kwargs["api_key"] = api_key

    private_key = resolve_kalshi_private_key(source.get("KALSHI_PRIVATE_KEY"))
    if private_key:
        kwargs["private_key"] = private_key

    return kwargs
