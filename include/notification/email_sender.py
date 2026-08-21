"""Reusable notification email delivery through Airflow SMTP."""

from dataclasses import dataclass

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.smtp.hooks.smtp import SmtpHook
from airflow.sdk import Variable, get_current_context


@dataclass(frozen=True)
class S3ReportEmailContext:
    """Values a DAG needs to compose an email for a report stored in S3."""

    recipient: str
    source_run_id: str
    s3_bucket: str
    report_key: str
    report_body: str


def load_s3_report_email_context(
    report_key: str,
    *,
    recipient_variable: str,
    bucket_variable: str,
    aws_conn_id: str = "aws_conn",
) -> S3ReportEmailContext:
    """Resolve Airflow values and download a UTF-8 report for email composition."""
    context = get_current_context()
    conf = dict(context["dag_run"].conf or {})
    recipient = Variable.get(recipient_variable)
    s3_bucket = Variable.get(bucket_variable)
    s3 = S3Hook(aws_conn_id=aws_conn_id)
    remote_report = s3.get_key(key=report_key, bucket_name=s3_bucket)
    if remote_report is None:
        raise FileNotFoundError(f"s3://{s3_bucket}/{report_key} does not exist")

    report_body = remote_report.get()["Body"].read().decode("utf-8")
    return S3ReportEmailContext(
        recipient=recipient,
        source_run_id=str(conf.get("source_run_id", "unknown")),
        s3_bucket=s3_bucket,
        report_key=report_key,
        report_body=report_body,
    )


def send_email(
    message: str,
    recipient: str,
    subject: str = "Airflow notification",
    smtp_conn_id: str = "smtp_default",
) -> None:
    """Send an HTML email to one recipient using an Airflow SMTP connection."""
    if not isinstance(recipient, str) or not recipient.strip():
        raise ValueError("recipient must be a non-empty email address")

    with SmtpHook(smtp_conn_id=smtp_conn_id) as smtp:
        smtp.send_email_smtp(
            to=recipient,
            subject=subject,
            html_content=message,
        )
