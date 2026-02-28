"""Airflow failure callbacks that publish alerts to AWS SNS."""

import json
import logging

import boto3
from airflow.models import Variable

logger = logging.getLogger(__name__)


def sns_on_failure(context: dict) -> None:
    """Publish a structured alert to SNS when a task fails.

    Reads the topic ARN from the Airflow Variable ``sns_alert_topic_arn``.
    """
    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    execution_date = str(context["execution_date"])
    exception = str(context.get("exception", ""))
    log_url = ti.log_url

    topic_arn = Variable.get("sns_alert_topic_arn")

    subject = f"[Airflow] FAILED — {dag_id}.{task_id}"
    message = {
        "dag_id": dag_id,
        "task_id": task_id,
        "execution_date": execution_date,
        "try_number": ti.try_number,
        "exception": exception[:1000],
        "log_url": log_url,
    }

    sns = boto3.client("sns")
    sns.publish(
        TopicArn=topic_arn,
        Subject=subject[:100],
        Message=json.dumps(message, indent=2),
    )
    logger.info("SNS alert sent to %s for %s.%s", topic_arn, dag_id, task_id)
