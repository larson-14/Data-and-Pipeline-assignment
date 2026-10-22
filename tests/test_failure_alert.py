"""Failure alerts cover both DAGs without exposing credentials or row data."""

import ast
import sys
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "include"))
from failure_alert import slack_failure_alert


class FailureAlertTests(unittest.TestCase):
    def context(self, conf=None):
        return {"dag_run": SimpleNamespace(dag_id="nammamart_etl_sudatta",
                                           run_id="test_run", conf=conf if conf is not None else {"send_notifications":True}),
                "exception": ValueError("private source row")}

    def variables(self, value=None, error=None):
        models = ModuleType("airflow.models")
        models.Variable = SimpleNamespace(get=Mock(return_value=value, side_effect=error))
        return {"airflow": ModuleType("airflow"), "airflow.models": models}

    def test_real_failure_reads_variable_and_sends_only_metadata(self):
        with patch.dict(sys.modules, self.variables("private-webhook")), \
                patch("failure_alert.send_slack_alert", return_value=True) as send:
            self.assertTrue(slack_failure_alert(self.context()))
        message = send.call_args.args[0]
        self.assertIn("test_run", message)
        self.assertNotIn("private", message)
        self.assertEqual(send.call_args.kwargs["webhook_url"], "private-webhook")

    def test_missing_variable_and_lookup_error_do_not_hide_failure(self):
        for modules in (self.variables(), self.variables(error=RuntimeError("private"))):
            with patch.dict(sys.modules, modules), patch("failure_alert.send_slack_alert") as send:
                self.assertFalse(slack_failure_alert(self.context()))
                send.assert_not_called()

    def test_experiments_do_not_send_external_messages(self):
        with patch("failure_alert.send_slack_alert") as send:
            self.assertFalse(slack_failure_alert(self.context({"observability_test": True})))
            send.assert_not_called()

    def test_both_dags_have_failure_callback(self):
        for pipeline in ("etl", "elt"):
            tree = ast.parse((ROOT / f"dags/nammamart_{pipeline}_sudatta.py").read_text(encoding="utf-8-sig"))
            self.assertTrue(any(isinstance(node, ast.keyword) and node.arg == "on_failure_callback"
                                and isinstance(node.value, ast.Name)
                                and node.value.id == "slack_failure_alert" for node in ast.walk(tree)))


if __name__ == "__main__":
    unittest.main()
