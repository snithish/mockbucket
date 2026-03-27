"""DuckDB parquet I/O helpers for compatibility tests."""

from __future__ import annotations

import duckdb


def s3_con(endpoint: str, key_id: str, secret: str, region: str = "us-east-1") -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection configured for an S3-compatible endpoint."""
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs")
    con.execute(f"SET s3_endpoint='{endpoint}'")
    con.execute(f"SET s3_access_key_id='{key_id}'")
    con.execute(f"SET s3_secret_access_key='{secret}'")
    con.execute(f"SET s3_region='{region}'")
    con.execute("SET s3_use_ssl=false")
    con.execute("SET s3_url_style='path'")
    con.execute("SET preserve_insertion_order=false")
    return con


def write_parquet_s3(
    con: duckdb.DuckDBPyConnection,
    base_uri: str,
    rows_per_file: int,
    num_files: int,
) -> list[str]:
    """Write num_files parquet files, each with rows_per_file rows."""
    uris = []
    for i in range(num_files):
        lo = i * rows_per_file
        hi = lo + rows_per_file - 1
        uri = f"{base_uri}/part_{i}.parquet"
        con.execute(
            f"COPY (SELECT i AS id, hash(i) AS val "
            f"FROM generate_series({lo}, {hi}) t(i)) "
            f"TO '{uri}' (FORMAT PARQUET)"
        )
        uris.append(uri)
    return uris


def read_count(con: duckdb.DuckDBPyConnection, uri: str) -> int:
    """Read parquet files (supports glob patterns) and return total row count."""
    return con.execute(f"SELECT count(*) FROM read_parquet('{uri}')").fetchone()[0]
