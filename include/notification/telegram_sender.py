"""Reusable Telegram notification delivery through Apprise."""

from collections.abc import Mapping
from typing import Any

from apprise import Apprise
from airflow.sdk import Variable


DEFAULT_TELEGRAM_URL_VARIABLE = "TELEGRAM_APPRISE_URL"


def send_telegram(
    message: str,
    title: str = "Airflow notification",
    *,
    url_variable: str = DEFAULT_TELEGRAM_URL_VARIABLE,
) -> None:
    """Send a Telegram notification using an Apprise URL in an Airflow Variable."""
    telegram_url = Variable.get(url_variable)
    if not isinstance(telegram_url, str) or not telegram_url.strip():
        raise ValueError(f"Airflow Variable {url_variable!r} must be a non-empty string")

    telegram_url = telegram_url.strip()
    if not telegram_url.lower().startswith("tgram://"):
        raise ValueError(
            f"Airflow Variable {url_variable!r} must contain a Telegram Apprise URL"
        )

    notifier = Apprise()
    if not notifier.add(telegram_url):
        raise ValueError(
            f"Airflow Variable {url_variable!r} contains an invalid Telegram "
            "Apprise URL"
        )

    if not notifier.notify(body=message, title=title):
        raise RuntimeError("Telegram notification delivery failed")


def notify_dag_failure(context: Mapping[str, Any]) -> None:
    """Send one Telegram notification when an Airflow DAG run fails."""
    dag_run = context.get("dag_run")
    task_instance = context.get("task_instance") or context.get("ti")

    dag_id = getattr(dag_run, "dag_id", None) or getattr(
        task_instance, "dag_id", "unknown"
    )
    run_id = getattr(dag_run, "run_id", None) or getattr(
        task_instance, "run_id", "unknown"
    )
    logical_date = getattr(dag_run, "logical_date", None) or context.get(
        "logical_date"
    )
    task_id = getattr(task_instance, "task_id", None)
    log_url = getattr(task_instance, "log_url", None)
    exception = context.get("exception") or context.get("reason")

    details = [
        f"DAG: {dag_id}",
        f"Run: {run_id}",
        f"Logical date: {logical_date or 'unknown'}",
    ]
    if task_id:
        details.append(f"Failed task: {task_id}")
    if exception:
        details.append(f"Error: {exception}")
    if log_url:
        details.append(f"Logs: {log_url}")

    send_telegram("\n".join(details), title=f"Airflow DAG failed: {dag_id}")
