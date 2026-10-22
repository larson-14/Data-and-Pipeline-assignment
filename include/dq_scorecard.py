"""Persist the ETL validation scorecard, keeping one result per check and run."""

import duckdb


def store_dq_results(database, results):
    if not results:
        return
    if any(not row["run_id"] or not row["dag_id"] for row in results):
        raise ValueError("Data quality results need the actual run_id and dag_id")
    con = duckdb.connect(str(database))
    try:
        con.execute("BEGIN TRANSACTION")
        con.execute("""
            CREATE TABLE IF NOT EXISTS dq_results (
                run_id VARCHAR, dag_id VARCHAR, check_name VARCHAR, dimension VARCHAR,
                severity VARCHAR, rows_checked BIGINT, rows_failed BIGINT,
                status VARCHAR, checked_at TIMESTAMPTZ
            )
        """)
        for row in results:
            con.execute("DELETE FROM dq_results WHERE run_id = ? AND dag_id = ? AND check_name = ?",
                        [row["run_id"], row["dag_id"], row["check_name"]])
            con.execute("INSERT INTO dq_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        [row[column] for column in ("run_id", "dag_id", "check_name", "dimension", "severity",
                                                   "rows_checked", "rows_failed", "status", "checked_at")])
            print(f"[DQ] {row['check_name']} | {row['dimension']} | {row['severity']} | "
                  f"{row['rows_failed']}/{row['rows_checked']} failed | {row['status']}")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
