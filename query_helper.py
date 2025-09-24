_GET_ALERTS_QUERY = """
    WITH current_location_hours AS (
        SELECT location_id, weekday, regular_open, regular_close
        FROM {hours_table}
        WHERE is_current
    )
    SELECT
        {closure_alerts_table}.location_id, name, alert_id, closed_for,
        extended_closing, alert_start, alert_end, polling_datetime,
        regular_open, regular_close
    FROM {closure_alerts_table} LEFT JOIN current_location_hours
    ON {closure_alerts_table}.location_id = current_location_hours.location_id
    AND TO_CHAR({closure_alerts_table}.polling_datetime AT TIME ZONE
        'America/New_York', 'Day') = current_location_hours.weekday;"""


def build_get_alerts_query(hours_table, closure_alerts_table):
    return _GET_ALERTS_QUERY.format(
        hours_table=hours_table, closure_alerts_table=closure_alerts_table
    )
