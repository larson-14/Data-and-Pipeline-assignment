"""ELT integration checks; run with python -m unittest tests.test_elt_pipeline."""

import ast
import contextlib
import csv
import io
import unittest
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb

from include.elt_warehouse import compare_september, load_raw_files, run_sql

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "include" / "data" / "nammamart"
SQL_DIR = ROOT / "include" / "sql"
NOW = datetime(2026, 10, 9, 12, tzinfo=timezone.utc)


class EltPipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = TemporaryDirectory()
        cls.database = Path(cls.temporary.name) / "elt.duckdb"
        cls.files = [DATA_DIR / (name + ".csv") for name in
                     ("orders", "payments", "customers", "products", "stores")]
        with contextlib.redirect_stdout(io.StringIO()):
            load_raw_files(cls.database, cls.files, "test_run")
            run_sql(cls.database, SQL_DIR,
                    ["stg_masters.sql", "stg_orders.sql", "dq_results.sql", "marts.sql", "baseline_snapshot.sql"],
                    "test_run", checked_at=NOW)
            load_raw_files(cls.database, [DATA_DIR / "orders_2026-10-01_increment.csv"], "test_run")
            run_sql(cls.database, SQL_DIR, ["stg_orders.sql", "dq_results.sql", "marts.sql"],
                    "test_run", phase="increment", checked_at=NOW)

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def test_raw_values_are_unchanged_and_all_columns_are_text(self):
        with duckdb.connect(str(self.database), read_only=True) as con:
            for path in self.files + [DATA_DIR / "orders_2026-10-01_increment.csv"]:
                table = "raw_orders" if path.name.startswith("orders_") else "raw_" + path.stem
                with path.open(encoding="utf-8-sig", newline="") as source:
                    reader = csv.reader(source)
                    headers, expected = next(reader), list(reader)
                columns = ", ".join('"' + name + '"' for name in headers)
                actual = con.execute(f"SELECT {columns} FROM {table} WHERE _source_file = ? ORDER BY rowid",
                                     [path.name]).fetchall()
                self.assertEqual(actual, [tuple(row) for row in expected])
                types = {row[0]: row[1] for row in con.execute(f"DESCRIBE {table}").fetchall()}
                self.assertTrue(all(types[name] == "VARCHAR" for name in headers))
                self.assertEqual(con.execute(f"SELECT COUNT(*) FROM {table} WHERE _run_id IS NULL OR _loaded_at IS NULL").fetchone()[0], 0)

    def test_increment_updates_ten_and_adds_one_hundred_fifty(self):
        with duckdb.connect(str(self.database), read_only=True) as con:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0],
                             con.execute("SELECT COUNT(*) + 150 FROM elt_base_orders").fetchone()[0])
            self.assertEqual(con.execute("""
                SELECT COUNT(*) FROM stg_orders current JOIN elt_base_orders old USING (order_id)
                WHERE current._source_file = 'orders_2026-10-01_increment.csv'
                  AND current.order_status = 'RETURNED'
            """).fetchone()[0], 10)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0],
                             con.execute("SELECT COUNT(DISTINCT order_id) FROM stg_orders").fetchone()[0])

    def test_retries_do_not_duplicate_raw_staging_marts_or_scorecard(self):
        with duckdb.connect(str(self.database), read_only=True) as con:
            before_raw = con.execute("SELECT COUNT(*) FROM raw_orders").fetchone()
            before_staging = con.execute("SELECT * REPLACE (CAST(_loaded_at AS VARCHAR) AS _loaded_at) FROM stg_orders ORDER BY order_id").fetchall()
            before_mart = con.execute("SELECT * FROM mart_daily_store_kpis ORDER BY ALL").fetchall()
            before_dq = con.execute("SELECT * REPLACE (CAST(checked_at AS VARCHAR) AS checked_at) FROM dq_results ORDER BY check_name").fetchall()
        with contextlib.redirect_stdout(io.StringIO()):
            load_raw_files(self.database, self.files + [DATA_DIR / "orders_2026-10-01_increment.csv"], "test_run")
            run_sql(self.database, SQL_DIR, ["stg_orders.sql", "dq_results.sql", "marts.sql"],
                    "test_run", phase="increment", checked_at=NOW)
        with duckdb.connect(str(self.database), read_only=True) as con:
            self.assertEqual(before_raw, con.execute("SELECT COUNT(*) FROM raw_orders").fetchone())
            self.assertEqual(before_staging, con.execute("SELECT * REPLACE (CAST(_loaded_at AS VARCHAR) AS _loaded_at) FROM stg_orders ORDER BY order_id").fetchall())
            self.assertEqual(before_mart, con.execute("SELECT * FROM mart_daily_store_kpis ORDER BY ALL").fetchall())
            self.assertEqual(before_dq, con.execute("SELECT * REPLACE (CAST(checked_at AS VARCHAR) AS checked_at) FROM dq_results ORDER BY check_name").fetchall())

    def test_quality_dimensions_warning_loading_and_masked_contacts(self):
        with duckdb.connect(str(self.database), read_only=True) as con:
            dimensions = {row[0] for row in con.execute("SELECT DISTINCT dimension FROM dq_results").fetchall()}
            self.assertEqual(dimensions, {"Completeness", "Uniqueness", "Validity", "Accuracy", "Consistency", "Timeliness"})
            self.assertGreater(con.execute("SELECT COUNT(*) FROM elt_reconciled_orders WHERE dq_status = 'Warning'").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM elt_reconciled_orders WHERE dq_status = 'Critical' OR order_status <> 'DELIVERED'").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM stg_customers WHERE NOT contains(email, '*') OR NOT contains(phone, '*')").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM stg_orders WHERE dq_status = 'Critical' AND coalesce(failure_reason, '') = ''").fetchone()[0], 0)
            self.assertGreater(con.execute("SELECT COUNT(*) FROM elt_quarantine WHERE check_name = 'order_quantity'").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM elt_quarantine WHERE severity <> 'Critical' OR NOT failed").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM stg_orders WHERE _source_row IS NULL OR _source_row < 1").fetchone()[0], 0)

    def test_mart_totals_agree_with_each_other_and_snapshot_preserves_september(self):
        with duckdb.connect(str(self.database), read_only=True) as con:
            store_total = con.execute("SELECT SUM(net_revenue), SUM(gross_margin) FROM mart_daily_store_kpis").fetchone()
            category_total = con.execute("SELECT SUM(net_revenue), SUM(gross_margin) FROM mart_category_margin").fetchone()
            self.assertAlmostEqual(store_total[0], category_total[0], places=6)
            self.assertAlmostEqual(store_total[1], category_total[1], places=6)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM mart_top_customers").fetchone()[0], 10)
            self.assertGreater(con.execute("SELECT COUNT(*) FROM elt_base_report_orders").fetchone()[0],
                               con.execute("SELECT COUNT(*) FROM elt_reconciled_orders WHERE order_date < DATE '2026-10-01'").fetchone()[0])

    def test_new_run_appends_raw_history_without_duplicating_current_orders(self):
        with TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()):
            db = Path(directory) / "history.duckdb"
            for run_id in ("first", "second"):
                load_raw_files(db, self.files, run_id)
                run_sql(db, SQL_DIR, ["stg_masters.sql", "stg_orders.sql", "dq_results.sql"], run_id, checked_at=NOW)
            with duckdb.connect(str(db)) as con:
                self.assertEqual(con.execute("SELECT COUNT(*) FROM raw_orders").fetchone()[0], 4824)
                self.assertEqual(con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0], 2400)
                self.assertEqual(con.execute("SELECT COUNT(DISTINCT run_id) FROM dq_results").fetchone()[0], 2)

    def test_sql_transactions_roll_back_partial_work(self):
        with TemporaryDirectory() as directory:
            scripts = Path(directory)
            (scripts / "failure.sql").write_text("CREATE TABLE partial_work (id INTEGER); SELECT * FROM missing_table;", encoding="utf-8")
            with self.assertRaises(duckdb.Error):
                run_sql(self.database, scripts, ["failure.sql"], "test_run")
            with duckdb.connect(str(self.database), read_only=True) as con:
                self.assertNotIn("partial_work", {row[0] for row in con.execute("SHOW TABLES").fetchall()})

    def test_empty_exports_still_record_every_check(self):
        with TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()):
            empty_dir = Path(directory)
            files = []
            for source in self.files:
                path = empty_dir / source.name
                with source.open(encoding='utf-8-sig', newline='') as handle:
                    headers = next(csv.reader(handle))
                with path.open('w', encoding='utf-8', newline='') as handle:
                    csv.writer(handle).writerow(headers)
                files.append(path)
            db = empty_dir / 'empty.duckdb'
            load_raw_files(db, files, 'empty')
            run_sql(db, SQL_DIR, ['stg_masters.sql', 'stg_orders.sql', 'dq_results.sql', 'marts.sql'],
                    'empty', checked_at=NOW)
            with duckdb.connect(str(db)) as con:
                self.assertGreater(con.execute('SELECT COUNT(*) FROM dq_results').fetchone()[0], 40)
                self.assertEqual(con.execute("SELECT COUNT(*) FROM dq_results WHERE rows_checked <> 0 OR rows_failed <> 0 OR status <> 'Passed'").fetchone()[0], 0)
                self.assertEqual(con.execute('SELECT COUNT(*) FROM mart_daily_store_kpis').fetchone()[0], 0)

    def test_changed_file_on_retry_preserves_the_original_raw_rows(self):
        with TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()):
            path = Path(directory) / 'orders.csv'
            path.write_bytes(self.files[0].read_bytes())
            db = Path(directory) / 'changed.duckdb'
            load_raw_files(db, [path], 'same_run')
            path.write_bytes(path.read_bytes().replace(b'UPI', b'upi', 1))
            with self.assertRaisesRegex(ValueError, 'changed during run'):
                load_raw_files(db, [path], 'same_run')
            with duckdb.connect(str(db)) as con:
                self.assertEqual(con.execute('SELECT COUNT(*) FROM raw_orders').fetchone()[0], 2412)

    def test_dag_syntax_and_task_dependencies(self):
        tree = ast.parse((ROOT / "dags" / "nammamart_elt_sudatta.py").read_text(encoding="utf-8"))
        wiring = next(node for node in tree.body if isinstance(node, ast.FunctionDef))
        self.assertEqual(wiring.name, "nammamart_elt_sudatta")
        tasks = {node.name for node in wiring.body if isinstance(node, ast.FunctionDef)}
        self.assertTrue({"load_raw", "transform_in_warehouse", "snapshot_september", "compare_etl_totals",
                         "load_increment", "upsert_increment", "build_marts"} <= tasks)

        edges = set()
        class Output:
            def __init__(self, name):
                self.name = name
            def __rshift__(self, other):
                edges.add((self.name, other.name))
                return other
        class Task:
            def __init__(self, name):
                self.name = name
            def override(self, task_id):
                return Task(task_id)
            def __call__(self, *args):
                for value in args:
                    for dependency in value if isinstance(value, list) else [value]:
                        if isinstance(dependency, Output):
                            edges.add((dependency.name, self.name))
                return Output(self.name)
        namespace = {name: Task(name) for name in tasks}
        statements = [node for node in wiring.body if not isinstance(node, ast.FunctionDef)]
        exec(compile(ast.Module(body=statements, type_ignores=[]), '<wiring>', 'exec'), namespace)
        expected = {(f'extract_{name}', 'load_raw') for name in ('orders', 'payments', 'customers', 'products', 'stores')}
        chain = ['load_raw', 'transform_in_warehouse', 'snapshot_september',
                 'load_increment', 'upsert_increment', 'build_marts', 'compare_etl_totals', 'run_audit']
        expected.update(zip(chain, chain[1:]))
        expected.update((name, 'run_audit') for name in chain[:-1])
        expected.update((f'extract_{name}', 'run_audit') for name in ('orders', 'payments', 'customers', 'products', 'stores'))
        self.assertEqual(edges, expected)

    def test_exact_september_differences_against_current_etl(self):
        from include.verify_elt import build_etl_reference
        with TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()):
            reference = Path(directory) / 'etl.duckdb'
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                build_etl_reference(reference)
            compare_september(self.database, SQL_DIR, reference, 'test_run')
            with duckdb.connect(str(self.database), read_only=True) as con:
                count, difference = con.execute('SELECT COUNT(*), coalesce(SUM(revenue_difference),0) FROM elt_september_comparison').fetchone()
                self.assertEqual(count, 0)
                self.assertAlmostEqual(difference, 0, places=6)
                self.assertEqual(con.execute("SELECT COUNT(*) FROM elt_september_comparison WHERE reason = 'order_delivery_required'").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
