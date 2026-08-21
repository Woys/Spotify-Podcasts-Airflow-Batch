from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from include.notification.email_sender import load_s3_report_email_context, send_email


@patch("include.notification.email_sender.S3Hook")
@patch("include.notification.email_sender.Variable")
@patch("include.notification.email_sender.get_current_context")
def test_load_s3_report_email_context(
    current_context: MagicMock,
    variable: MagicMock,
    s3_hook: MagicMock,
) -> None:
    current_context.return_value = {
        "dag_run": MagicMock(conf={"source_run_id": "scheduled__2026-08-20"})
    }
    variable.get.side_effect = ["owner@example.com", "reports-bucket"]
    remote_report = s3_hook.return_value.get_key.return_value
    remote_report.get.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=b'{"summary": "failed"}'))
    }

    result = load_s3_report_email_context(
        "qa/run-1/qa_report.json",
        recipient_variable="DBT_QA_EMAIL",
        bucket_variable="SP_S3_BUCKET",
        aws_conn_id="custom-aws",
    )

    assert result.recipient == "owner@example.com"
    assert result.source_run_id == "scheduled__2026-08-20"
    assert result.s3_bucket == "reports-bucket"
    assert result.report_key == "qa/run-1/qa_report.json"
    assert result.report_body == '{"summary": "failed"}'
    s3_hook.assert_called_once_with(aws_conn_id="custom-aws")
    s3_hook.return_value.get_key.assert_called_once_with(
        key="qa/run-1/qa_report.json",
        bucket_name="reports-bucket",
    )


@patch("include.notification.email_sender.S3Hook")
@patch("include.notification.email_sender.Variable")
@patch("include.notification.email_sender.get_current_context")
def test_load_s3_report_email_context_requires_report(
    current_context: MagicMock,
    variable: MagicMock,
    s3_hook: MagicMock,
) -> None:
    current_context.return_value = {"dag_run": MagicMock(conf={})}
    variable.get.side_effect = ["owner@example.com", "reports-bucket"]
    s3_hook.return_value.get_key.return_value = None

    with pytest.raises(
        FileNotFoundError,
        match="s3://reports-bucket/qa/missing.json does not exist",
    ):
        load_s3_report_email_context(
            "qa/missing.json",
            recipient_variable="DBT_QA_EMAIL",
            bucket_variable="SP_S3_BUCKET",
        )


@patch("include.notification.email_sender.SmtpHook")
def test_send_email_uses_default_settings(smtp_hook: MagicMock) -> None:
    smtp = smtp_hook.return_value.__enter__.return_value

    result = send_email("<p>Pipeline completed</p>", "owner@example.com")

    assert result is None
    smtp_hook.assert_called_once_with(smtp_conn_id="smtp_default")
    smtp.send_email_smtp.assert_called_once_with(
        to="owner@example.com",
        subject="Airflow notification",
        html_content="<p>Pipeline completed</p>",
    )
    smtp_hook.return_value.__exit__.assert_called_once()


@patch("include.notification.email_sender.SmtpHook")
def test_send_email_accepts_subject_and_connection_overrides(
    smtp_hook: MagicMock,
) -> None:
    smtp = smtp_hook.return_value.__enter__.return_value

    send_email(
        "<strong>dbt tests failed</strong>",
        "analytics@example.com",
        subject="dbt failure",
        smtp_conn_id="team_smtp",
    )

    smtp_hook.assert_called_once_with(smtp_conn_id="team_smtp")
    smtp.send_email_smtp.assert_called_once_with(
        to="analytics@example.com",
        subject="dbt failure",
        html_content="<strong>dbt tests failed</strong>",
    )


@pytest.mark.parametrize("recipient", ["", "   ", None])
@patch("include.notification.email_sender.SmtpHook")
def test_send_email_rejects_empty_recipient(
    smtp_hook: MagicMock,
    recipient: str | None,
) -> None:
    with pytest.raises(ValueError, match="recipient must be a non-empty email address"):
        send_email("message", recipient)  # type: ignore[arg-type]

    smtp_hook.assert_not_called()


@patch("include.notification.email_sender.SmtpHook")
def test_send_email_propagates_delivery_errors(smtp_hook: MagicMock) -> None:
    smtp = smtp_hook.return_value.__enter__.return_value
    smtp.send_email_smtp.side_effect = RuntimeError("SMTP unavailable")

    with pytest.raises(RuntimeError, match="SMTP unavailable"):
        send_email("message", "owner@example.com")
