# ruff: noqa: B008
import argparse
import asyncio
import hashlib
import logging
import os
import re
import signal
import sys
import time
from enum import Enum
from typing import Any
from typing import List
from typing import Literal
from typing import Union

import mcp.types as types
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

# The lowlevel server sets this contextvar for the duration of every request, so
# any code in the tool call stack (e.g. get_sql_driver) can read the incoming
# HTTP request — and thus the per-request connection headers — without threading
# a Context parameter through every tool. Imported defensively: on transports
# without an HTTP request (stdio) request_ctx.get() raises LookupError / .request
# is None, which we treat as "not connection-from-request".
try:
    from mcp.server.lowlevel.server import request_ctx as _request_ctx
except Exception:  # pragma: no cover - defensive
    _request_ctx = None
from pydantic import Field
from pydantic import validate_call

from postgres_mcp.index.dta_calc import DatabaseTuningAdvisor

from .artifacts import ErrorResult
from .artifacts import ExplainPlanArtifact
from .database_health import DatabaseHealthTool
from .database_health import HealthType
from .explain import ExplainPlanTool
from .index.index_opt_base import MAX_NUM_INDEX_TUNING_QUERIES
from .index.llm_opt import LLMOptimizerTool
from .index.presentation import TextPresentation
from .sql import DbConnPool
from .sql import SafeSqlDriver
from .sql import SqlDriver
from .sql import check_hypopg_installation_status
from .sql import obfuscate_password
from .top_queries import TopQueriesCalc

# Initialize FastMCP with default settings
mcp = FastMCP("postgres-mcp")

# Constants
PG_STAT_STATEMENTS = "pg_stat_statements"
HYPOPG_EXTENSION = "hypopg"

ResponseType = List[types.TextContent | types.ImageContent | types.EmbeddedResource]

logger = logging.getLogger(__name__)


class AccessMode(str, Enum):
    """SQL access modes for the server."""

    UNRESTRICTED = "unrestricted"  # Unrestricted access
    RESTRICTED = "restricted"  # Read-only with safety features


# Global variables
db_connection = DbConnPool()
current_access_mode = AccessMode.UNRESTRICTED
shutdown_in_progress = False

# Per-request-connection mode (stackblaze fork). When True the process is NOT
# bound to one DATABASE_URI at boot; instead each request carries its target via
# the X-Kubero-DB-URI header (and X-Kubero-Access-Mode), so a single long-lived
# Deployment serves every Postgres add-on in the cluster. The kubero broker
# resolves the connection per chat-selected add-on and sets these headers.
connection_from_request = False

# Header names the broker sets per request.
HDR_DB_URI = "x-kubero-db-uri"
HDR_ACCESS_MODE = "x-kubero-access-mode"


class _ConnRegistry:
    """Lazily-created pool of DbConnPools keyed by connection URI.

    One process serves many add-on DBs (connection-from-request mode). Pools are
    created on first use for a URI and evicted after an idle period so we don't
    hold connections to add-ons no longer in any chat context. Bounded so a busy
    instance can't open unbounded pools.
    """

    def __init__(self, max_idle_seconds: int = 300, max_pools: int = 32):
        self._pools: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._max_idle = max_idle_seconds
        self._max_pools = max_pools

    @staticmethod
    def _key(uri: str) -> str:
        return hashlib.sha256(uri.encode("utf-8")).hexdigest()

    async def get(self, uri: str) -> DbConnPool:
        key = self._key(uri)
        async with self._lock:
            await self._evict_idle_locked()
            entry = self._pools.get(key)
            if entry is None:
                if len(self._pools) >= self._max_pools:
                    await self._evict_oldest_locked()
                pool = DbConnPool()
                await pool.pool_connect(uri)
                entry = {"pool": pool}
                self._pools[key] = entry
            entry["last_used"] = time.monotonic()
            return entry["pool"]

    async def _evict_idle_locked(self) -> None:
        now = time.monotonic()
        stale = [k for k, e in self._pools.items() if now - e.get("last_used", now) > self._max_idle]
        for k in stale:
            await self._close_entry(self._pools.pop(k))

    async def _evict_oldest_locked(self) -> None:
        if not self._pools:
            return
        oldest = min(self._pools.items(), key=lambda kv: kv[1].get("last_used", 0))
        await self._close_entry(self._pools.pop(oldest[0]))

    @staticmethod
    async def _close_entry(entry: dict[str, Any]) -> None:
        try:
            await entry["pool"].close()
        except Exception as e:  # pragma: no cover - best effort
            logger.warning(f"Error closing pooled connection: {e}")

    async def close_all(self) -> None:
        async with self._lock:
            for entry in self._pools.values():
                await self._close_entry(entry)
            self._pools.clear()


conn_registry = _ConnRegistry()


def _request_headers():
    """Return the incoming request's headers (case-insensitive) or None.

    None means there is no HTTP request in scope (e.g. stdio transport), in which
    case the caller falls back to the process-global connection / access mode.
    """
    if _request_ctx is None:
        return None
    try:
        rc = _request_ctx.get()
    except LookupError:
        return None
    req = getattr(rc, "request", None)
    if req is None:
        return None
    return getattr(req, "headers", None)


def _effective_access_mode() -> AccessMode:
    """Access mode for the current call.

    In connection-from-request mode it comes from the per-request header and
    defaults to RESTRICTED when missing/invalid (fail safe — a production add-on
    must never silently get write access)."""
    if connection_from_request:
        headers = _request_headers()
        raw = headers.get(HDR_ACCESS_MODE) if headers else None
        if not raw:
            return AccessMode.RESTRICTED
        try:
            return AccessMode(raw)
        except ValueError:
            return AccessMode.RESTRICTED
    return current_access_mode


async def get_sql_driver() -> Union[SqlDriver, SafeSqlDriver]:
    """Get the appropriate SQL driver for this call.

    Connection-from-request: resolve the per-URI pool + access mode from the
    request headers. Otherwise: the process-global pool + boot access mode."""
    access_mode = current_access_mode
    conn: DbConnPool = db_connection

    if connection_from_request:
        headers = _request_headers()
        uri = headers.get(HDR_DB_URI) if headers else None
        if not uri:
            raise ValueError(f"connection-from-request mode: missing {HDR_DB_URI} header")
        access_mode = _effective_access_mode()
        conn = await conn_registry.get(uri)

    base_driver = SqlDriver(conn=conn)

    if access_mode == AccessMode.RESTRICTED:
        logger.debug("Using SafeSqlDriver with restrictions (RESTRICTED mode)")
        return SafeSqlDriver(sql_driver=base_driver, timeout=30)  # 30 second timeout
    else:
        logger.debug("Using unrestricted SqlDriver (UNRESTRICTED mode)")
        return base_driver


def _require_unrestricted() -> ResponseType | None:
    """Guard for WRITE tools. In connection-from-request mode the access mode is
    per-request, so write tools are always registered but must refuse when the
    selected add-on is restricted (e.g. a production phase). Returns an error
    response to short-circuit, or None when the write is allowed."""
    if _effective_access_mode() == AccessMode.RESTRICTED:
        return format_error_response(
            "This operation requires unrestricted access; the selected add-on is read-only (restricted) mode."
        )
    return None


def format_text_response(text: Any) -> ResponseType:
    """Format a text response."""
    return [types.TextContent(type="text", text=str(text))]


def format_error_response(error: str) -> ResponseType:
    """Format an error response."""
    return format_text_response(f"Error: {error}")


@mcp.tool(
    description="List all schemas in the database",
    annotations=ToolAnnotations(
        title="List Schemas",
        readOnlyHint=True,
    ),
)
async def list_schemas() -> ResponseType:
    """List all schemas in the database."""
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(
            """
            SELECT
                schema_name,
                schema_owner,
                CASE
                    WHEN schema_name LIKE 'pg_%' THEN 'System Schema'
                    WHEN schema_name = 'information_schema' THEN 'System Information Schema'
                    ELSE 'User Schema'
                END as schema_type
            FROM information_schema.schemata
            ORDER BY schema_type, schema_name
            """
        )
        schemas = [row.cells for row in rows] if rows else []
        return format_text_response(schemas)
    except Exception as e:
        logger.error(f"Error listing schemas: {e}")
        return format_error_response(str(e))


@mcp.tool(
    description="List objects in a schema",
    annotations=ToolAnnotations(
        title="List Objects",
        readOnlyHint=True,
    ),
)
async def list_objects(
    schema_name: str = Field(description="Schema name"),
    object_type: str = Field(description="Object type: 'table', 'view', 'sequence', or 'extension'", default="table"),
) -> ResponseType:
    """List objects of a given type in a schema."""
    try:
        sql_driver = await get_sql_driver()

        if object_type in ("table", "view"):
            table_type = "BASE TABLE" if object_type == "table" else "VIEW"
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_schema = {} AND table_type = {}
                ORDER BY table_name
                """,
                [schema_name, table_type],
            )
            objects = (
                [{"schema": row.cells["table_schema"], "name": row.cells["table_name"], "type": row.cells["table_type"]} for row in rows]
                if rows
                else []
            )

        elif object_type == "sequence":
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT sequence_schema, sequence_name, data_type
                FROM information_schema.sequences
                WHERE sequence_schema = {}
                ORDER BY sequence_name
                """,
                [schema_name],
            )
            objects = (
                [{"schema": row.cells["sequence_schema"], "name": row.cells["sequence_name"], "data_type": row.cells["data_type"]} for row in rows]
                if rows
                else []
            )

        elif object_type == "extension":
            # Extensions are not schema-specific
            rows = await sql_driver.execute_query(
                """
                SELECT extname, extversion, extrelocatable
                FROM pg_extension
                ORDER BY extname
                """
            )
            objects = (
                [{"name": row.cells["extname"], "version": row.cells["extversion"], "relocatable": row.cells["extrelocatable"]} for row in rows]
                if rows
                else []
            )

        else:
            return format_error_response(f"Unsupported object type: {object_type}")

        return format_text_response(objects)
    except Exception as e:
        logger.error(f"Error listing objects: {e}")
        return format_error_response(str(e))


@mcp.tool(
    description="Show detailed information about a database object",
    annotations=ToolAnnotations(
        title="Get Object Details",
        readOnlyHint=True,
    ),
)
async def get_object_details(
    schema_name: str = Field(description="Schema name"),
    object_name: str = Field(description="Object name"),
    object_type: str = Field(description="Object type: 'table', 'view', 'sequence', or 'extension'", default="table"),
) -> ResponseType:
    """Get detailed information about a database object."""
    try:
        sql_driver = await get_sql_driver()

        if object_type in ("table", "view"):
            # Get columns
            col_rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = {} AND table_name = {}
                ORDER BY ordinal_position
                """,
                [schema_name, object_name],
            )
            columns = (
                [
                    {
                        "column": r.cells["column_name"],
                        "data_type": r.cells["data_type"],
                        "is_nullable": r.cells["is_nullable"],
                        "default": r.cells["column_default"],
                    }
                    for r in col_rows
                ]
                if col_rows
                else []
            )

            # Get constraints
            con_rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT tc.constraint_name, tc.constraint_type, kcu.column_name
                FROM information_schema.table_constraints AS tc
                LEFT JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema = kcu.table_schema
                WHERE tc.table_schema = {} AND tc.table_name = {}
                """,
                [schema_name, object_name],
            )

            constraints = {}
            if con_rows:
                for row in con_rows:
                    cname = row.cells["constraint_name"]
                    ctype = row.cells["constraint_type"]
                    col = row.cells["column_name"]

                    if cname not in constraints:
                        constraints[cname] = {"type": ctype, "columns": []}
                    if col:
                        constraints[cname]["columns"].append(col)

            constraints_list = [{"name": name, **data} for name, data in constraints.items()]

            # Get indexes
            idx_rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = {} AND tablename = {}
                """,
                [schema_name, object_name],
            )

            indexes = [{"name": r.cells["indexname"], "definition": r.cells["indexdef"]} for r in idx_rows] if idx_rows else []

            result = {
                "basic": {"schema": schema_name, "name": object_name, "type": object_type},
                "columns": columns,
                "constraints": constraints_list,
                "indexes": indexes,
            }

        elif object_type == "sequence":
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT sequence_schema, sequence_name, data_type, start_value, increment
                FROM information_schema.sequences
                WHERE sequence_schema = {} AND sequence_name = {}
                """,
                [schema_name, object_name],
            )

            if rows and rows[0]:
                row = rows[0]
                result = {
                    "schema": row.cells["sequence_schema"],
                    "name": row.cells["sequence_name"],
                    "data_type": row.cells["data_type"],
                    "start_value": row.cells["start_value"],
                    "increment": row.cells["increment"],
                }
            else:
                result = {}

        elif object_type == "extension":
            rows = await SafeSqlDriver.execute_param_query(
                sql_driver,
                """
                SELECT extname, extversion, extrelocatable
                FROM pg_extension
                WHERE extname = {}
                """,
                [object_name],
            )

            if rows and rows[0]:
                row = rows[0]
                result = {"name": row.cells["extname"], "version": row.cells["extversion"], "relocatable": row.cells["extrelocatable"]}
            else:
                result = {}

        else:
            return format_error_response(f"Unsupported object type: {object_type}")

        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting object details: {e}")
        return format_error_response(str(e))


@mcp.tool(
    description="Explains the execution plan for a SQL query, showing how the database will execute it and provides detailed cost estimates.",
    annotations=ToolAnnotations(
        title="Explain Query",
        readOnlyHint=True,
    ),
)
async def explain_query(
    sql: str = Field(description="SQL query to explain"),
    analyze: bool = Field(
        description="When True, actually runs the query to show real execution statistics instead of estimates. "
        "Takes longer but provides more accurate information.",
        default=False,
    ),
    hypothetical_indexes: list[dict[str, Any]] = Field(
        description="""A list of hypothetical indexes to simulate. Each index must be a dictionary with these keys:
    - 'table': The table name to add the index to (e.g., 'users')
    - 'columns': List of column names to include in the index (e.g., ['email'] or ['last_name', 'first_name'])
    - 'using': Optional index method (default: 'btree', other options include 'hash', 'gist', etc.)

Examples: [
    {"table": "users", "columns": ["email"], "using": "btree"},
    {"table": "orders", "columns": ["user_id", "created_at"]}
]
If there is no hypothetical index, you can pass an empty list.""",
        default=[],
    ),
) -> ResponseType:
    """
    Explains the execution plan for a SQL query.

    Args:
        sql: The SQL query to explain
        analyze: When True, actually runs the query for real statistics
        hypothetical_indexes: Optional list of indexes to simulate
    """
    try:
        sql_driver = await get_sql_driver()
        explain_tool = ExplainPlanTool(sql_driver=sql_driver)
        result: ExplainPlanArtifact | ErrorResult | None = None

        # If hypothetical indexes are specified, check for HypoPG extension
        if hypothetical_indexes and len(hypothetical_indexes) > 0:
            if analyze:
                return format_error_response("Cannot use analyze and hypothetical indexes together")
            try:
                # Use the common utility function to check if hypopg is installed
                (
                    is_hypopg_installed,
                    hypopg_message,
                ) = await check_hypopg_installation_status(sql_driver)

                # If hypopg is not installed, return the message
                if not is_hypopg_installed:
                    return format_text_response(hypopg_message)

                # HypoPG is installed, proceed with explaining with hypothetical indexes
                result = await explain_tool.explain_with_hypothetical_indexes(sql, hypothetical_indexes)
            except Exception:
                raise  # Re-raise the original exception
        elif analyze:
            try:
                # Use EXPLAIN ANALYZE
                result = await explain_tool.explain_analyze(sql)
            except Exception:
                raise  # Re-raise the original exception
        else:
            try:
                # Use basic EXPLAIN
                result = await explain_tool.explain(sql)
            except Exception:
                raise  # Re-raise the original exception

        if result and isinstance(result, ExplainPlanArtifact):
            return format_text_response(result.to_text())
        else:
            error_message = "Error processing explain plan"
            if isinstance(result, ErrorResult):
                error_message = result.to_text()
            return format_error_response(error_message)
    except Exception as e:
        logger.error(f"Error explaining query: {e}")
        return format_error_response(str(e))


# Query function declaration without the decorator - we'll add it dynamically based on access mode
async def execute_sql(
    sql: str = Field(description="SQL to run", default="all"),
) -> ResponseType:
    """Executes a SQL query against the database."""
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(sql)  # type: ignore
        if rows is None:
            return format_text_response("No results")
        return format_text_response(list([r.cells for r in rows]))
    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return format_error_response(str(e))


@mcp.tool(
    description="Analyze frequently executed queries in the database and recommend optimal indexes",
    annotations=ToolAnnotations(
        title="Analyze Workload Indexes",
        readOnlyHint=True,
    ),
)
@validate_call
async def analyze_workload_indexes(
    max_index_size_mb: int = Field(description="Max index size in MB", default=10000),
    method: Literal["dta", "llm"] = Field(description="Method to use for analysis", default="dta"),
) -> ResponseType:
    """Analyze frequently executed queries in the database and recommend optimal indexes."""
    try:
        sql_driver = await get_sql_driver()
        if method == "dta":
            index_tuning = DatabaseTuningAdvisor(sql_driver)
        else:
            index_tuning = LLMOptimizerTool(sql_driver)
        dta_tool = TextPresentation(sql_driver, index_tuning)
        result = await dta_tool.analyze_workload(max_index_size_mb=max_index_size_mb)
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error analyzing workload: {e}")
        return format_error_response(str(e))


@mcp.tool(
    description="Analyze a list of (up to 10) SQL queries and recommend optimal indexes",
    annotations=ToolAnnotations(
        title="Analyze Query Indexes",
        readOnlyHint=True,
    ),
)
@validate_call
async def analyze_query_indexes(
    queries: list[str] = Field(description="List of Query strings to analyze"),
    max_index_size_mb: int = Field(description="Max index size in MB", default=10000),
    method: Literal["dta", "llm"] = Field(description="Method to use for analysis", default="dta"),
) -> ResponseType:
    """Analyze a list of SQL queries and recommend optimal indexes."""
    if len(queries) == 0:
        return format_error_response("Please provide a non-empty list of queries to analyze.")
    if len(queries) > MAX_NUM_INDEX_TUNING_QUERIES:
        return format_error_response(f"Please provide a list of up to {MAX_NUM_INDEX_TUNING_QUERIES} queries to analyze.")

    try:
        sql_driver = await get_sql_driver()
        if method == "dta":
            index_tuning = DatabaseTuningAdvisor(sql_driver)
        else:
            index_tuning = LLMOptimizerTool(sql_driver)
        dta_tool = TextPresentation(sql_driver, index_tuning)
        result = await dta_tool.analyze_queries(queries=queries, max_index_size_mb=max_index_size_mb)
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error analyzing queries: {e}")
        return format_error_response(str(e))


@mcp.tool(
    description="Analyzes database health. Here are the available health checks:\n"
    "- index - checks for invalid, duplicate, and bloated indexes\n"
    "- connection - checks the number of connection and their utilization\n"
    "- vacuum - checks vacuum health for transaction id wraparound\n"
    "- sequence - checks sequences at risk of exceeding their maximum value\n"
    "- replication - checks replication health including lag and slots\n"
    "- buffer - checks for buffer cache hit rates for indexes and tables\n"
    "- constraint - checks for invalid constraints\n"
    "- all - runs all checks\n"
    "You can optionally specify a single health check or a comma-separated list of health checks. The default is 'all' checks.",
    annotations=ToolAnnotations(
        title="Analyze Database Health",
        readOnlyHint=True,
    ),
)
async def analyze_db_health(
    health_type: str = Field(
        description=f"Optional. Valid values are: {', '.join(sorted([t.value for t in HealthType]))}.",
        default="all",
    ),
) -> ResponseType:
    """Analyze database health for specified components.

    Args:
        health_type: Comma-separated list of health check types to perform.
                    Valid values: index, connection, vacuum, sequence, replication, buffer, constraint, all
    """
    health_tool = DatabaseHealthTool(await get_sql_driver())
    result = await health_tool.health(health_type=health_type)
    return format_text_response(result)


@mcp.tool(
    name="get_top_queries",
    description=f"Reports the slowest or most resource-intensive queries using data from the '{PG_STAT_STATEMENTS}' extension.",
    annotations=ToolAnnotations(
        title="Get Top Queries",
        readOnlyHint=True,
    ),
)
async def get_top_queries(
    sort_by: str = Field(
        description="Ranking criteria: 'total_time' for total execution time or 'mean_time' for mean execution time per call, or 'resources' "
        "for resource-intensive queries",
        default="resources",
    ),
    limit: int = Field(description="Number of queries to return when ranking based on mean_time or total_time", default=10),
) -> ResponseType:
    try:
        sql_driver = await get_sql_driver()
        top_queries_tool = TopQueriesCalc(sql_driver=sql_driver)

        if sort_by == "resources":
            result = await top_queries_tool.get_top_resource_queries()
            return format_text_response(result)
        elif sort_by == "mean_time" or sort_by == "total_time":
            # Map the sort_by values to what get_top_queries_by_time expects
            result = await top_queries_tool.get_top_queries_by_time(limit=limit, sort_by="mean" if sort_by == "mean_time" else "total")
        else:
            return format_error_response("Invalid sort criteria. Please use 'resources' or 'mean_time' or 'total_time'.")
        return format_text_response(result)
    except Exception as e:
        logger.error(f"Error getting slow queries: {e}")
        return format_error_response(str(e))


# --------------------------------------------------------------------------- #
# Discrete DBA tools (stackblaze fork). The upstream exposes analysis + a single
# `execute_sql`; these add explicit, validated buttons for common day-2 admin so
# the agent doesn't hand-write DDL. READ tools register always; WRITE tools are
# registered only in UNRESTRICTED mode (see main) — so a `production` add-on
# spawned `--access-mode restricted` is analyse/query only.
# --------------------------------------------------------------------------- #

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,62}$")


def _ident(name: str) -> str:
    """Validate a SQL identifier (role/database name) and return it double-quoted.

    Strict allowlist — rejects anything that isn't a plain identifier — so it is
    safe to interpolate into DDL where bind params aren't allowed."""
    if not isinstance(name, str) or not _IDENT_RE.match(name):
        raise ValueError(f"Invalid identifier: {name!r} (expected letters, digits, underscore; ≤63 chars)")
    return '"' + name + '"'


@mcp.tool(
    description="List database roles/users with their key attributes (login, superuser, createdb, createrole, connection limit).",
    annotations=ToolAnnotations(title="List Roles", readOnlyHint=True),
)
async def list_roles() -> ResponseType:
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(
            """
            SELECT rolname, rolsuper, rolcreatedb, rolcreaterole,
                   rolcanlogin, rolreplication, rolconnlimit, rolvaliduntil
            FROM pg_roles ORDER BY rolname
            """
        )
        return format_text_response([r.cells for r in rows] if rows else [])
    except Exception as e:
        logger.error(f"Error listing roles: {e}")
        return format_error_response(str(e))


@mcp.tool(
    description="List databases in this cluster with owner and encoding.",
    annotations=ToolAnnotations(title="List Databases", readOnlyHint=True),
)
async def list_databases() -> ResponseType:
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(
            """
            SELECT d.datname,
                   pg_catalog.pg_get_userbyid(d.datdba) AS owner,
                   pg_catalog.pg_encoding_to_char(d.encoding) AS encoding,
                   pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(d.datname)) AS size
            FROM pg_catalog.pg_database d
            WHERE d.datistemplate = false
            ORDER BY d.datname
            """
        )
        return format_text_response([r.cells for r in rows] if rows else [])
    except Exception as e:
        logger.error(f"Error listing databases: {e}")
        return format_error_response(str(e))


@mcp.tool(
    description="List active backend sessions (pid, user, database, state, wait, and a truncated current query).",
    annotations=ToolAnnotations(title="List Active Sessions", readOnlyHint=True),
)
async def list_active_sessions() -> ResponseType:
    try:
        sql_driver = await get_sql_driver()
        rows = await sql_driver.execute_query(
            """
            SELECT pid, usename, datname, state, wait_event_type, wait_event,
                   xact_start, query_start, left(query, 120) AS query
            FROM pg_stat_activity
            WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()
            ORDER BY query_start NULLS LAST
            """
        )
        return format_text_response([r.cells for r in rows] if rows else [])
    except Exception as e:
        logger.error(f"Error listing sessions: {e}")
        return format_error_response(str(e))


# --- WRITE tools (registered only in UNRESTRICTED mode) --------------------- #

async def create_role(
    role_name: str = Field(description="Name of the role/user to create (plain identifier)"),
    password: str | None = Field(description="Password for the role; omit for a no-login/no-password role", default=None),
    can_login: bool = Field(description="Grant LOGIN (i.e. a usable user)", default=True),
    can_create_db: bool = Field(description="Grant CREATEDB", default=False),
) -> ResponseType:
    """Create a database role/user."""
    denied = _require_unrestricted()
    if denied is not None:
        return denied
    try:
        ident = _ident(role_name)
        opts = ["LOGIN" if can_login else "NOLOGIN"]
        if can_create_db:
            opts.append("CREATEDB")
        if password is not None:
            # DDL can't bind params for PASSWORD; escape the literal safely.
            esc = password.replace("'", "''")
            opts.append(f"PASSWORD '{esc}'")
        sql = f"CREATE ROLE {ident} {' '.join(opts)}"
        sql_driver = await get_sql_driver()
        await sql_driver.execute_query(sql)
        return format_text_response(f"Role {role_name} created.")
    except Exception as e:
        logger.error(f"Error creating role: {e}")
        return format_error_response(str(e))


async def drop_role(
    role_name: str = Field(description="Name of the role to drop (plain identifier)"),
) -> ResponseType:
    """Drop a database role (IF EXISTS)."""
    denied = _require_unrestricted()
    if denied is not None:
        return denied
    try:
        ident = _ident(role_name)
        sql_driver = await get_sql_driver()
        await sql_driver.execute_query(f"DROP ROLE IF EXISTS {ident}")
        return format_text_response(f"Role {role_name} dropped (if it existed).")
    except Exception as e:
        logger.error(f"Error dropping role: {e}")
        return format_error_response(str(e))


async def terminate_session(
    pid: int = Field(description="Backend PID to terminate (from list_active_sessions)"),
) -> ResponseType:
    """Terminate a backend session by pid (pg_terminate_backend)."""
    denied = _require_unrestricted()
    if denied is not None:
        return denied
    try:
        if not isinstance(pid, int):
            return format_error_response("pid must be an integer")
        sql_driver = await get_sql_driver()
        rows = await SafeSqlDriver.execute_param_query(
            sql_driver, "SELECT pg_terminate_backend({}) AS terminated", [pid]
        )
        return format_text_response([r.cells for r in rows] if rows else "No result")
    except Exception as e:
        logger.error(f"Error terminating session: {e}")
        return format_error_response(str(e))


async def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="PostgreSQL MCP Server")
    parser.add_argument("database_url", help="Database connection URL", nargs="?")
    parser.add_argument(
        "--access-mode",
        type=str,
        choices=[mode.value for mode in AccessMode],
        default=AccessMode.UNRESTRICTED.value,
        help="Set SQL access mode: unrestricted (unrestricted) or restricted (read-only with protections)",
    )
    parser.add_argument(
        "--transport",
        type=str,
        choices=["stdio", "sse", "streamable-http"],
        default="stdio",
        help="Select MCP transport: stdio (default), sse, or streamable-http",
    )
    parser.add_argument(
        "--connection-from-request",
        action="store_true",
        help=(
            "Per-request connection mode (stackblaze fork). Do not bind to one "
            "DATABASE_URI at boot; instead read the target per request from the "
            f"'{HDR_DB_URI}' header and the access mode from '{HDR_ACCESS_MODE}'. "
            "Lets one long-lived Deployment serve every Postgres add-on. Requires "
            "an HTTP transport (streamable-http/sse)."
        ),
    )
    parser.add_argument(
        "--sse-host",
        type=str,
        default="localhost",
        help="Host to bind SSE server to (default: localhost)",
    )
    parser.add_argument(
        "--sse-port",
        type=int,
        default=8000,
        help="Port for SSE server (default: 8000)",
    )
    parser.add_argument(
        "--streamable-http-host",
        type=str,
        default="localhost",
        help="Host to bind streamable HTTP server to (default: localhost)",
    )
    parser.add_argument(
        "--streamable-http-port",
        type=int,
        default=8000,
        help="Port for streamable HTTP server (default: 8000)",
    )

    args = parser.parse_args()

    # Store the access mode + connection mode in the global variables
    global current_access_mode, connection_from_request
    current_access_mode = AccessMode(args.access_mode)
    connection_from_request = bool(args.connection_from_request)

    if connection_from_request and args.transport == "stdio":
        raise ValueError("--connection-from-request requires an HTTP transport (streamable-http or sse), not stdio.")

    # Register the query + write tools.
    #
    # connection-from-request: ONE process serves both restricted (e.g. prod) and
    # unrestricted (dev/review) callers, decided per request — so we can't gate by
    # registration. Register the full surface; access is enforced per call by
    # get_sql_driver() (SafeSqlDriver) + the _require_unrestricted() write guards.
    #
    # single-DB (legacy/standalone): keep upstream behaviour — write tools only in
    # UNRESTRICTED mode so a restricted process never exposes them at all.
    register_writes = connection_from_request or current_access_mode == AccessMode.UNRESTRICTED

    if register_writes:
        mcp.add_tool(
            execute_sql,
            description="Execute any SQL query",
            annotations=ToolAnnotations(
                title="Execute SQL",
                destructiveHint=True,
            ),
        )
        mcp.add_tool(
            create_role,
            description="Create a database role/user",
            annotations=ToolAnnotations(title="Create Role"),
        )
        mcp.add_tool(
            drop_role,
            description="Drop a database role",
            annotations=ToolAnnotations(title="Drop Role", destructiveHint=True),
        )
        mcp.add_tool(
            terminate_session,
            description="Terminate a backend session by pid",
            annotations=ToolAnnotations(title="Terminate Session", destructiveHint=True),
        )
    else:
        mcp.add_tool(
            execute_sql,
            description="Execute a read-only SQL query",
            annotations=ToolAnnotations(
                title="Execute SQL (Read-Only)",
                readOnlyHint=True,
            ),
        )

    mode_label = "CONNECTION-FROM-REQUEST" if connection_from_request else current_access_mode.upper()
    logger.info(f"Starting PostgreSQL MCP Server in {mode_label} mode")

    # Get database URL from environment variable or command line
    database_url = os.environ.get("DATABASE_URI", args.database_url)

    if connection_from_request:
        # No process-global connection; per-URI pools are created on demand from
        # the request header. A boot DATABASE_URI (if any) is ignored.
        if database_url:
            logger.info("connection-from-request mode: ignoring boot DATABASE_URI; connections come from request headers")
    else:
        if not database_url:
            raise ValueError(
                "Error: No database URL provided. Please specify via 'DATABASE_URI' environment variable or command-line argument.",
            )
        # Initialize database connection pool
        try:
            await db_connection.pool_connect(database_url)
            logger.info("Successfully connected to database and initialized connection pool")
        except Exception as e:
            logger.warning(
                f"Could not connect to database: {obfuscate_password(str(e))}",
            )
            logger.warning(
                "The MCP server will start but database operations will fail until a valid connection is established.",
            )

    # Set up proper shutdown handling
    try:
        loop = asyncio.get_running_loop()
        signals = (signal.SIGTERM, signal.SIGINT)
        for s in signals:
            loop.add_signal_handler(s, lambda s=s: asyncio.create_task(shutdown(s)))
    except NotImplementedError:
        # Windows doesn't support signals properly
        logger.warning("Signal handling not supported on Windows")
        pass

    # Run the server with the selected transport (always async)
    if args.transport == "stdio":
        await mcp.run_stdio_async()
    elif args.transport == "sse":
        mcp.settings.host = args.sse_host
        mcp.settings.port = args.sse_port
        await mcp.run_sse_async()
    elif args.transport == "streamable-http":
        mcp.settings.host = args.streamable_http_host
        mcp.settings.port = args.streamable_http_port
        await mcp.run_streamable_http_async()


async def shutdown(sig=None):
    """Clean shutdown of the server."""
    global shutdown_in_progress

    if shutdown_in_progress:
        logger.warning("Forcing immediate exit")
        # Use sys.exit instead of os._exit to allow for proper cleanup
        sys.exit(1)

    shutdown_in_progress = True

    if sig:
        logger.info(f"Received exit signal {sig.name}")

    # Close database connections
    try:
        await db_connection.close()
        await conn_registry.close_all()
        logger.info("Closed database connections")
    except Exception as e:
        logger.error(f"Error closing database connections: {e}")

    # Exit with appropriate status code
    sys.exit(128 + sig if sig is not None else 0)
