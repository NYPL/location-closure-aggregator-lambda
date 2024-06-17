import json
import os
import pandas as pd

from datetime import datetime
from nypl_py_utils.classes.kms_client import KmsClient
from nypl_py_utils.classes.redshift_client import RedshiftClient
from nypl_py_utils.functions.log_helper import create_log
from pytz import timezone
from query_helper import build_get_alerts_query

logger = create_log('lambda_function')

_EASTERN_TIMEZONE = timezone('US/Eastern')


def get_closures(alerts_df):
    logger.info('Aggregating closures')
    if len(alerts_df) == 0:
        return None

    # Each polling session should only encompass one day
    polling_datetimes = alerts_df['polling_datetime'].unique()
    polling_date = polling_datetimes.min().astimezone('US/Eastern').date()
    if (polling_date !=
            polling_datetimes.max().astimezone('US/Eastern').date()):
        logger.error('Polling occurred over multiple days')
        raise LocationClosureAggregatorError(
            'Polling occurred over multiple days')

    closures = []
    for ids, alert_group in alerts_df.groupby(['alert_id',
                                               'drupal_location_id']):
        # These are fake alerts created by the LocationClosureAlertPoller for
        # the purpose of recording each polling datetime
        if ids[0] == 'location_closure_alert_poller':
            continue

        # We assume the most recently polled version of the alert is the most
        # accurate and use it as the primary data source
        last_alert = alert_group.loc[alert_group['polling_datetime'].idxmax()]
        alert_start_et = last_alert['alert_start'].astimezone(
            'US/Eastern')
        alert_end_et = last_alert['alert_end'].astimezone('US/Eastern')
        closure = {
            'drupal_location_id': last_alert['drupal_location_id'],
            'name': last_alert['name'],
            'alert_id': last_alert['alert_id'],
            'closed_for': last_alert['closed_for'],
            'is_extended_closure': last_alert['extended_closing'],
            'closure_date': polling_date.isoformat()
        }

        # If the library's regular hours are not available (e.g. when the
        # library is under an extended closure), check that the alert was
        # active on the polling date and, if so, assume it lasts the full day
        # and record only the date of the closure without times
        if (last_alert['regular_open'] is None or
                last_alert['regular_close'] is None):
            if (alert_start_et.date() <= polling_date and
                    alert_end_et.date() >= polling_date):
                closure['closure_start'] = None
                closure['closure_end'] = None
                closure['is_full_day'] = True
                closures.append(closure)
            continue

        regular_open_et = _EASTERN_TIMEZONE.localize(
            datetime.combine(polling_date, last_alert['regular_open']))
        regular_close_et = _EASTERN_TIMEZONE.localize(
            datetime.combine(polling_date, last_alert['regular_close']))

        # Ignore alerts that occur outside of a library's regular hours
        if (alert_start_et < regular_close_et
                and alert_end_et > regular_open_et):
            # Clamp the closure to the library's regular hours
            closure_start = max(regular_open_et, alert_start_et)
            closure_end = min(regular_close_et, alert_end_et)

            # If the stated closure doesn't match what's seen by the
            # poller, infer the real closure from the polling times.
            alert_poll_times = set(alert_group['polling_datetime'])
            expected_alert_poll_times = {
                dt for dt in polling_datetimes
                if dt > closure_start and dt < closure_end}
            regular_hours_poll_times = {
                dt for dt in polling_datetimes
                if dt > regular_open_et and dt < regular_close_et}
            if not expected_alert_poll_times.issubset(alert_poll_times):
                closure_start = min(
                    alert_poll_times).astimezone('US/Eastern')
                closure_end = max(
                    alert_poll_times).astimezone('US/Eastern')
            closure['closure_start'] = closure_start.time().isoformat()
            closure['closure_end'] = closure_end.time().isoformat()
            closure['is_full_day'] = regular_hours_poll_times.issubset(
                alert_poll_times)
            closures.append(closure)

    return None if len(closures) == 0 else pd.DataFrame.from_dict(
        closures).values.tolist()


def lambda_handler(event, context):
    logger.info('Starting lambda processing')
    kms_client = KmsClient()
    redshift_client = RedshiftClient(
        kms_client.decrypt(os.environ['REDSHIFT_DB_HOST']),
        os.environ['REDSHIFT_DB_NAME'],
        kms_client.decrypt(os.environ['REDSHIFT_DB_USER']),
        kms_client.decrypt(os.environ['REDSHIFT_DB_PASSWORD']))
    kms_client.close()

    hours_table = 'location_hours'
    closures_table = 'location_closures'
    closure_alerts_table = 'location_closure_alerts'
    if os.environ['REDSHIFT_DB_NAME'] != 'production':
        db_suffix = '_{}'.format(os.environ['REDSHIFT_DB_NAME'])
        hours_table += db_suffix
        closures_table += db_suffix
        closure_alerts_table += db_suffix

    redshift_client.connect()
    raw_alerts = redshift_client.execute_query(
        build_get_alerts_query(hours_table, closure_alerts_table))
    alerts_df = pd.DataFrame(data=raw_alerts, columns=[
        'drupal_location_id', 'name', 'alert_id', 'closed_for',
        'extended_closing', 'alert_start', 'alert_end', 'polling_datetime',
        'regular_open', 'regular_close'])
    closures = get_closures(alerts_df)
    queries = []
    if closures is not None:
        placeholder = ", ".join(["%s"] * len(closures[0]))
        insert_query = 'INSERT INTO {closures_table} VALUES ({placeholder});'\
            .format(closures_table=closures_table, placeholder=placeholder)
        queries.append((insert_query, closures))
    queries.append(('DELETE FROM {};'.format(closure_alerts_table), None))
    redshift_client.execute_transaction(queries)
    redshift_client.close_connection()

    logger.info('Finished lambda processing')
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Job ran successfully."
        })
    }


class LocationClosureAggregatorError(Exception):
    def __init__(self, message=None):
        self.message = message
