"""Unit tests for the stackblaze fork's connection-from-request mode.

One long-lived Deployment serves every Postgres add-on: the target DB + access
mode arrive per request as HTTP headers (X-Kubero-DB-URI / X-Kubero-Access-Mode),
resolved against a per-URI pool registry. These tests cover the resolution +
safety logic without a real Postgres (the pool's pool_connect is mocked)."""

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

import postgres_mcp.server as server
from postgres_mcp.server import AccessMode
from postgres_mcp.server import _ConnRegistry
from postgres_mcp.server import _effective_access_mode
from postgres_mcp.server import _require_unrestricted
from postgres_mcp.server import get_sql_driver
from postgres_mcp.sql.safe_sql import SafeSqlDriver
from postgres_mcp.sql.sql_driver import SqlDriver


class _Headers(dict):
    """Minimal case-insensitive .get, like starlette's Headers."""

    def get(self, key, default=None):  # type: ignore[override]
        return super().get(key.lower(), default)


def _with_headers(headers):
    """Patch _request_headers to return the given headers and enable the mode."""
    return (
        patch("postgres_mcp.server.connection_from_request", True),
        patch("postgres_mcp.server._request_headers", lambda: _Headers(headers) if headers is not None else None),
    )


# --- access mode resolution (fail-safe) ------------------------------------ #


def test_effective_access_mode_defaults_restricted_when_missing():
    p1, p2 = _with_headers({"x-kubero-db-uri": "postgres://u:p@h/db"})  # no access-mode header
    with p1, p2:
        assert _effective_access_mode() == AccessMode.RESTRICTED


def test_effective_access_mode_invalid_is_restricted():
    p1, p2 = _with_headers({"x-kubero-access-mode": "garbage"})
    with p1, p2:
        assert _effective_access_mode() == AccessMode.RESTRICTED


@pytest.mark.parametrize(
    "raw,expected",
    [("unrestricted", AccessMode.UNRESTRICTED), ("restricted", AccessMode.RESTRICTED)],
)
def test_effective_access_mode_parses_header(raw, expected):
    p1, p2 = _with_headers({"x-kubero-access-mode": raw})
    with p1, p2:
        assert _effective_access_mode() == expected


# --- get_sql_driver per-request -------------------------------------------- #


@pytest.mark.asyncio
async def test_get_sql_driver_missing_uri_header_raises():
    p1, p2 = _with_headers({})  # connection-from-request on, but no db-uri header
    with p1, p2:
        with pytest.raises(ValueError, match="x-kubero-db-uri"):
            await get_sql_driver()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode,driver_type",
    [("unrestricted", SqlDriver), ("restricted", SafeSqlDriver)],
)
async def test_get_sql_driver_uses_request_pool_and_mode(mode, driver_type):
    fake_pool = MagicMock()
    p1, p2 = _with_headers({"x-kubero-db-uri": "postgres://u:p@h:5432/db", "x-kubero-access-mode": mode})
    with p1, p2, patch.object(server.conn_registry, "get", AsyncMock(return_value=fake_pool)) as get_mock:
        driver = await get_sql_driver()
        get_mock.assert_awaited_once_with("postgres://u:p@h:5432/db")
        assert isinstance(driver, driver_type)


# --- write-tool guard ------------------------------------------------------- #


def test_require_unrestricted_blocks_restricted():
    p1, p2 = _with_headers({"x-kubero-access-mode": "restricted"})
    with p1, p2:
        denied = _require_unrestricted()
        assert denied is not None
        assert "restricted" in denied[0].text.lower()


def test_require_unrestricted_allows_unrestricted():
    p1, p2 = _with_headers({"x-kubero-access-mode": "unrestricted"})
    with p1, p2:
        assert _require_unrestricted() is None


# --- per-URI pool registry -------------------------------------------------- #


@pytest.mark.asyncio
async def test_conn_registry_one_pool_per_uri():
    created = []

    def make_pool():
        m = MagicMock()
        m.pool_connect = AsyncMock()
        m.close = AsyncMock()
        created.append(m)
        return m

    reg = _ConnRegistry()
    with patch("postgres_mcp.server.DbConnPool", side_effect=make_pool):
        a1 = await reg.get("postgres://a")
        a2 = await reg.get("postgres://a")  # same URI → same pool, no second connect
        b1 = await reg.get("postgres://b")  # different URI → new pool

    assert a1 is a2
    assert b1 is not a1
    assert len(created) == 2
    a1.pool_connect.assert_awaited_once_with("postgres://a")


@pytest.mark.asyncio
async def test_conn_registry_evicts_idle():
    def make_pool():
        m = MagicMock()
        m.pool_connect = AsyncMock()
        m.close = AsyncMock()
        return m

    reg = _ConnRegistry(max_idle_seconds=-1)  # everything counts as idle (deterministic)
    with patch("postgres_mcp.server.DbConnPool", side_effect=make_pool):
        first = await reg.get("postgres://a")
        # next get() runs eviction first; the idle 'a' pool is closed
        await reg.get("postgres://b")

    first.close.assert_awaited()
