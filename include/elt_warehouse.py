"""Raw CSV loading and SQL execution for the Nammamart ELT pipeline."""

import csv
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import duckdb
try:
    from include.data_contract import check_schema
except ImportError:
    from data_contract import check_schema


def load_raw_files(database, files, run_id):
    """Append unchanged CSV fields; a retry of the same file/run is a no-op."""
    con = duckdb.connect(str(database))
    try:
        con.execute("BEGIN TRANSACTION")
        con.execute("""
            CREATE TABLE IF NOT EXISTS elt_raw_loads (
                run_id VARCHAR, source_file VARCHAR, file_hash VARCHAR, rows_loaded BIGINT,
                PRIMARY KEY (run_id, source_file)
            )
        """)
        for filename in files:
            path = Path(filename)
            table = "raw_orders" if path.name.startswith("orders_") else "raw_" + path.stem
            if table not in {"raw_orders", "raw_payments", "raw_customers", "raw_products", "raw_stores"}:
                raise ValueError(f"Unexpected input file: {path.name}")
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            previous = con.execute(
                "SELECT file_hash FROM elt_raw_loads WHERE run_id = ? AND source_file = ?",
                [run_id, path.name],
            ).fetchone()
            if previous:
                if previous[0] != digest:
                    raise ValueError(f"[Raw] {path.name} changed during run {run_id}")
                print(f"[Raw] {path.name} already loaded for this run. Skipping.")
                continue
            with path.open(encoding="utf-8-sig", newline="") as source:
                reader = csv.reader(source)
                columns = next(reader)
                rows = list(reader)
            check_schema('orders' if path.name.startswith('orders') else path.stem, columns)
            if any(len(row) != len(columns) for row in rows):
                raise ValueError(f"[Raw] Unexpected CSV field count in {path.name}")
            definitions = ", ".join('"' + column.replace('"', '""') + '" VARCHAR' for column in columns)
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    {definitions}, _loaded_at TIMESTAMPTZ, _source_file VARCHAR, _run_id VARCHAR
                )
            """)
            existing_columns = [row[0] for row in con.execute(f"DESCRIBE {table}").fetchall()][:-3]
            if columns != existing_columns:
                raise ValueError(f"[Raw][Schema] {path.name}: expected {existing_columns}, received {columns}")
            loaded_at = datetime.now(timezone.utc)
            if rows:
                placeholders = ", ".join("?" for _ in range(len(columns) + 3))
                con.executemany(
                    f"INSERT INTO {table} VALUES ({placeholders})",
                    [row + [loaded_at, path.name, run_id] for row in rows],
                )
            con.execute("INSERT INTO elt_raw_loads VALUES (?, ?, ?, ?)",
                        [run_id, path.name, digest, len(rows)])
            print(f"[Raw] {len(rows)} unchanged rows appended to {table} from {path.name}")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def run_sql(database, sql_dir, scripts, run_id, phase="base", checked_at=None, etl_database=None):
    """Run a layer in one transaction; all business transformations stay in SQL."""
    con = duckdb.connect(str(database))
    try:
        con.execute("SET TimeZone = 'Asia/Kolkata'")
        if etl_database is not None:
            # ATTACH accepts a literal path; escape quotes rather than interpolating SQL input.
            path = str(Path(etl_database).resolve()).replace("'", "''")
            con.execute(f"ATTACH '{path}' AS etl_reference (READ_ONLY)")
        con.execute("BEGIN TRANSACTION")
        con.execute("""
            CREATE TEMP TABLE pipeline_context AS
            SELECT ?::VARCHAR AS run_id, 'nammamart_elt_sudatta' AS dag_id,
                   ?::VARCHAR AS phase, ?::TIMESTAMPTZ AS checked_at
        """, [run_id, phase, checked_at or datetime.now(timezone.utc)])
        for script in scripts:
            con.execute((Path(sql_dir) / script).read_text(encoding="utf-8"))
            print(f"[SQL] {script} completed ({phase})")
        if "dq_results.sql" in scripts:
            for row in con.execute("""
                SELECT check_name, dimension, severity, rows_checked, rows_failed, status
                FROM dq_results WHERE run_id = ? AND starts_with(check_name, ?)
                ORDER BY check_name
            """, [run_id, phase + "."]).fetchall():
                print("[DQ]", row)
        if "batch_observability.sql" in scripts:
            for row in con.execute("SELECT check_name, status FROM dq_results WHERE run_id=? AND check_name IN ('order_volume','order_freshness')",
                                   [run_id]).fetchall():
                print("[DQ]", row)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def compare_september(database, sql_dir, etl_database, run_id):
    """Compare the existing ETL warehouse without changing any ETL tables."""
    if not Path(etl_database).is_file():
        raise FileNotFoundError("ETL warehouse unavailable; complete ETL before requesting comparison")
    reference = duckdb.connect(str(etl_database), read_only=True)
    try:
        tables = {row[0] for row in reference.execute("SHOW TABLES").fetchall()}
    finally:
        reference.close()
    required = {"etl_fact_orders", "etl_dim_product", "etl_dim_store", "etl_payment_reconciliation"}
    if not required <= tables:
        raise ValueError(f"ETL warehouse is missing tables: {sorted(required - tables)}")
    run_sql(database, sql_dir, ["compare_september.sql"], run_id, etl_database=etl_database)
    con = duckdb.connect(str(database), read_only=True)
    try:
        summary = con.execute("""
            SELECT count(*), coalesce(sum(revenue_difference), 0),
                   coalesce(sum(elt_gross_margin - etl_gross_margin), 0)
            FROM elt_september_comparison
        """).fetchone()
        print(f"[Compare] September: {summary[0]} different orders; revenue delta {summary[1]:.2f}; margin delta {summary[2]:.2f}")
        for row in con.execute("SELECT reason, count(*), sum(revenue_difference) FROM elt_september_comparison GROUP BY reason ORDER BY reason").fetchall():
            print(row)
        if summary[0]:
            raise ValueError(f"September parity failed for {summary[0]} orders; see elt_september_comparison")
    finally:
        con.close()
