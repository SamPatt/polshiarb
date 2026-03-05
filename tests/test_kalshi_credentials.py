from __future__ import annotations

from pathlib import Path

import app.kalshi_credentials as kalshi_credentials
from app.kalshi_credentials import build_kalshi_client_kwargs, resolve_kalshi_private_key


def test_resolve_kalshi_private_key_reads_relative_file(tmp_path: Path, monkeypatch) -> None:
    key_text = """-----BEGIN PRIVATE KEY-----
abc123
-----END PRIVATE KEY-----
"""
    key_path = tmp_path / "key.pem"
    key_path.write_text(key_text)
    monkeypatch.chdir(tmp_path)

    resolved = resolve_kalshi_private_key("./key.pem")
    assert resolved == key_text


def test_resolve_kalshi_private_key_keeps_inline_pem() -> None:
    pem_value = "-----BEGIN PRIVATE KEY-----\\nabc123\\n-----END PRIVATE KEY-----"

    resolved = resolve_kalshi_private_key(pem_value)
    assert resolved == pem_value


def test_resolve_kalshi_private_key_returns_raw_for_missing_path() -> None:
    missing_path = "./does-not-exist.pem"

    resolved = resolve_kalshi_private_key(missing_path)
    assert resolved == missing_path


def test_build_kalshi_client_kwargs_from_env_path(tmp_path: Path, monkeypatch) -> None:
    key_text = """-----BEGIN PRIVATE KEY-----
abc123
-----END PRIVATE KEY-----
"""
    key_path = tmp_path / "key.pem"
    key_path.write_text(key_text)

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("KALSHI_API_KEY", "demo-key-id")
    monkeypatch.setenv("KALSHI_PRIVATE_KEY", "./key.pem")

    kwargs = build_kalshi_client_kwargs()

    assert kwargs["api_key"] == "demo-key-id"
    assert kwargs["private_key"] == key_text


def test_build_kalshi_client_kwargs_from_dotenv(tmp_path: Path, monkeypatch) -> None:
    key_text = """-----BEGIN PRIVATE KEY-----
abc123
-----END PRIVATE KEY-----
"""
    key_path = tmp_path / "key.pem"
    key_path.write_text(key_text)
    (tmp_path / ".env").write_text(
        "KALSHI_API_KEY=dotenv-key-id\nKALSHI_PRIVATE_KEY=./key.pem\n",
        encoding="utf-8",
    )

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(kalshi_credentials, "_REPO_ROOT", tmp_path)
    monkeypatch.delenv("KALSHI_API_KEY", raising=False)
    monkeypatch.delenv("KALSHI_PRIVATE_KEY", raising=False)

    kwargs = build_kalshi_client_kwargs()

    assert kwargs["api_key"] == "dotenv-key-id"
    assert kwargs["private_key"] == key_text
