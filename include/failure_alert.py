"""Slack notification when either Nammamart DAG finishes in failure."""

import logging

from slack_notifier import send_slack_alert

log = logging.getLogger(__name__)


def slack_failure_alert(context):
    """Read the webhook at runtime; alert independently of the AI task."""
    dag_run = context["dag_run"]
    if (dag_run.conf or {}).get("observability_test") or not (dag_run.conf or {}).get("send_notifications", False):
        log.info("[Slack] Observability experiment: failure alert recorded in console")
        return False

    try:
        from airflow.models import Variable
        webhook_url = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
        if not webhook_url:
            log.warning("[Slack] Failure alert not sent: SLACK_WEBHOOK_URL is not configured")
            return False

        # Do not include exceptions or source rows: they can contain secrets or PII.
        message = (
            "NammaMart pipeline failed\n"
            f"DAG: {dag_run.dag_id}\n"
            f"Run: {dag_run.run_id}\n"
            "Open the failed run in Airflow to review task logs and retry the failed tasks."
        )
        sent = send_slack_alert(message, webhook_url=webhook_url)
        if not sent:
            log.warning("[Slack] Failure notification was not delivered; check webhook connectivity")
        return sent
    except Exception as error:
        log.warning("[Slack] Failure notification could not run: %s", type(error).__name__)
        return False
