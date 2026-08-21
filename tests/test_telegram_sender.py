from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from include.notification.telegram_sender import notify_dag_failure, send_telegram


@patch("include.notification.telegram_sender.send_telegram")
def test_notify_dag_failure_includes_run_and_failure_details(
    telegram: MagicMock,
) -> None:
    context = {
        "dag_run": SimpleNamespace(
            dag_id="example_dag",
            run_id="scheduled__2026-08-21",
            logical_date=datetime(2026, 8, 21, tzinfo=timezone.utc),
        ),
        "task_instance": SimpleNamespace(
            task_id="load_data",
            log_url="https://airflow.example/log",
        ),
        "exception": RuntimeError("source unavailable"),
    }

    notify_dag_failure(context)

    telegram.assert_called_once()
    message = telegram.call_args.args[0]
    assert "DAG: example_dag" in message
    assert "Run: scheduled__2026-08-21" in message
    assert "Failed task: load_data" in message
    assert "Error: source unavailable" in message
    assert "Logs: https://airflow.example/log" in message
    assert telegram.call_args.kwargs == {"title": "Airflow DAG failed: example_dag"}


@patch("include.notification.telegram_sender.Apprise")
@patch("include.notification.telegram_sender.Variable")
def test_send_telegram_uses_default_settings(
    variable: MagicMock,
    apprise: MagicMock,
) -> None:
    variable.get.return_value = "tgram://bot-token/123456"
    notifier = apprise.return_value
    notifier.add.return_value = True
    notifier.notify.return_value = True

    result = send_telegram("Pipeline completed")

    assert result is None
    variable.get.assert_called_once_with("TELEGRAM_APPRISE_URL")
    notifier.add.assert_called_once_with("tgram://bot-token/123456")
    notifier.notify.assert_called_once_with(
        body="Pipeline completed",
        title="Airflow notification",
    )


@patch("include.notification.telegram_sender.Apprise")
@patch("include.notification.telegram_sender.Variable")
def test_send_telegram_accepts_title_and_variable_overrides(
    variable: MagicMock,
    apprise: MagicMock,
) -> None:
    variable.get.return_value = "  tgram://bot-token/987654?format=markdown  "
    notifier = apprise.return_value
    notifier.add.return_value = True
    notifier.notify.return_value = True

    send_telegram(
        "dbt tests failed",
        title="dbt failure",
        url_variable="TEAM_TELEGRAM_URL",
    )

    variable.get.assert_called_once_with("TEAM_TELEGRAM_URL")
    notifier.add.assert_called_once_with(
        "tgram://bot-token/987654?format=markdown"
    )
    notifier.notify.assert_called_once_with(
        body="dbt tests failed",
        title="dbt failure",
    )


@pytest.mark.parametrize("telegram_url", ["", "   ", None])
@patch("include.notification.telegram_sender.Apprise")
@patch("include.notification.telegram_sender.Variable")
def test_send_telegram_rejects_empty_url(
    variable: MagicMock,
    apprise: MagicMock,
    telegram_url: str | None,
) -> None:
    variable.get.return_value = telegram_url

    with pytest.raises(ValueError, match="must be a non-empty string"):
        send_telegram("message")

    apprise.assert_not_called()


@patch("include.notification.telegram_sender.Apprise")
@patch("include.notification.telegram_sender.Variable")
def test_send_telegram_propagates_missing_variable(
    variable: MagicMock,
    apprise: MagicMock,
) -> None:
    variable.get.side_effect = KeyError("TELEGRAM_APPRISE_URL")

    with pytest.raises(KeyError, match="TELEGRAM_APPRISE_URL"):
        send_telegram("message")

    apprise.assert_not_called()


@patch("include.notification.telegram_sender.Apprise")
@patch("include.notification.telegram_sender.Variable")
def test_send_telegram_rejects_non_telegram_url(
    variable: MagicMock,
    apprise: MagicMock,
) -> None:
    variable.get.return_value = "discord://secret-webhook"

    with pytest.raises(ValueError, match="must contain a Telegram Apprise URL") as exc:
        send_telegram("message")

    assert "secret-webhook" not in str(exc.value)
    apprise.assert_not_called()


@patch("include.notification.telegram_sender.Apprise")
@patch("include.notification.telegram_sender.Variable")
def test_send_telegram_rejects_unparseable_url_without_exposing_it(
    variable: MagicMock,
    apprise: MagicMock,
) -> None:
    variable.get.return_value = "tgram://secret-token/invalid-chat"
    apprise.return_value.add.return_value = False

    with pytest.raises(ValueError, match="invalid Telegram Apprise URL") as exc:
        send_telegram("message")

    assert "secret-token" not in str(exc.value)
    apprise.return_value.notify.assert_not_called()


@patch("include.notification.telegram_sender.Apprise")
@patch("include.notification.telegram_sender.Variable")
def test_send_telegram_raises_when_delivery_fails(
    variable: MagicMock,
    apprise: MagicMock,
) -> None:
    variable.get.return_value = "tgram://bot-token/123456"
    notifier = apprise.return_value
    notifier.add.return_value = True
    notifier.notify.return_value = False

    with pytest.raises(RuntimeError, match="Telegram notification delivery failed"):
        send_telegram("message")


@patch("include.notification.telegram_sender.Apprise")
@patch("include.notification.telegram_sender.Variable")
def test_send_telegram_propagates_delivery_errors(
    variable: MagicMock,
    apprise: MagicMock,
) -> None:
    variable.get.return_value = "tgram://bot-token/123456"
    notifier = apprise.return_value
    notifier.add.return_value = True
    notifier.notify.side_effect = RuntimeError("Telegram unavailable")

    with pytest.raises(RuntimeError, match="Telegram unavailable"):
        send_telegram("message")
