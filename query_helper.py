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

_INSERT_QUERY = """
    INSERT INTO {closures_table} (
        location_id, name, alert_id, closed_for, is_extended_closure, closure_date,
        closure_start, closure_end, is_full_day
    ) VALUES ({placeholder});"""


def build_get_alerts_query(hours_table, closure_alerts_table):
    return _GET_ALERTS_QUERY.format(
        hours_table=hours_table, closure_alerts_table=closure_alerts_table
    )


def build_insert_query(closures_table, placeholder):
    return _INSERT_QUERY.format(closures_table=closures_table, placeholder=placeholder)
