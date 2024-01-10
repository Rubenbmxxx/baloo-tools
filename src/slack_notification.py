from airflow.hooks.base_hook import BaseHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import pendulum

SLACK_CONN_ID = 'slack_connection'


def convert_datetime(datetime_string):
    return datetime_string.astimezone(pendulum.timezone("Europe/Amsterdam")).strftime('%b-%d %H:%M:%S')


# Slack Alert
def slack_fail_alert(context):
    """Adapted from https://medium.com/datareply/integrating-slack-alerts-in-airflow-c9dcd155105
         Sends message to a slack channel.
            If you want to send it to a "user" -> use "@user",
                if "public channel" -> use "#channel",
                if "private channel" -> use "channel"
                :param context:
                :return:
    """

    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = f"""
        :x: Task Failed.
        *Task*: {context.get('task_instance').task_id}
        *Dag*: {context.get('task_instance').dag_id}
        *Execution Time*: {convert_datetime(context.get('logical_date'))}
        <{context.get('task_instance').log_url}|*Logs*>
    """

    slack_alert = SlackWebhookOperator(
        task_id='slack_fail',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow',
        http_conn_id=SLACK_CONN_ID
    )

    return slack_alert.execute(context=context)
