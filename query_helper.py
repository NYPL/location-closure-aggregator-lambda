_GET_ALERTS_QUERY = """
    WITH current_location_hours AS (
        SELECT
            {hours_table}.drupal_location_id,
            {hours_table}.weekday,
            {hours_table}.regular_open,
            {hours_table}.regular_close
        FROM {hours_table} INNER JOIN (
            SELECT drupal_location_id, weekday,
                MAX(date_of_change) AS last_date
            FROM {hours_table}
            GROUP BY drupal_location_id, weekday
        ) x
        ON {hours_table}.drupal_location_id = x.drupal_location_id
        AND {hours_table}.weekday = x.weekday
        AND (
            {hours_table}.date_of_change = x.last_date OR
            (x.last_date IS NULL AND {hours_table}.date_of_change IS NULL)
        )
    )
    SELECT
        {closure_alerts_table}.drupal_location_id, name, alert_id, closed_for,
        extended_closing, alert_start, alert_end, polling_datetime,
        regular_open, regular_close
    FROM {closure_alerts_table} LEFT JOIN current_location_hours
    ON {closure_alerts_table}.drupal_location_id =
        current_location_hours.drupal_location_id
    AND LEFT(TO_CHAR({closure_alerts_table}.polling_datetime AT TIME ZONE
        'America/New_York', 'Day'), 3) = current_location_hours.weekday;"""


def build_get_alerts_query(hours_table, closure_alerts_table):
    return _GET_ALERTS_QUERY.format(
        hours_table=hours_table, closure_alerts_table=closure_alerts_table
    )
