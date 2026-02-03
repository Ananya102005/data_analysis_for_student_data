from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="fact_attendance_daily_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=True,
    tags=["fact", "attendance"]
) as dag:

    # ------------------------------------------------------------
    # Create FACT table
    # ------------------------------------------------------------
    create_fact_table = PostgresOperator(
        task_id="create_fact_table",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS fact_attendance_daily (
            badge_id TEXT NOT NULL,
            date_id DATE NOT NULL,

            total_hours REAL,
            total_sessions INTEGER,

            first_in_time TIMESTAMP,
            last_out_time TIMESTAMP,

            avg_session_hours REAL,

            PRIMARY KEY (badge_id, date_id)
        );
        """
    )

    # ------------------------------------------------------------
    # Populate FACT table (overnight-safe, session-aware)
    # ------------------------------------------------------------
    calculate_daily_attendance = PostgresOperator(
        task_id="calculate_daily_attendance",
        postgres_conn_id="postgres_default",
        sql="""
        -- Idempotent reload for the day
        DELETE FROM fact_attendance_daily
        WHERE date_id = '{{ ds }}'::date;

        WITH ordered_events AS (
            SELECT
                badge_id,
                event_time,
                event_type,
                LEAD(event_time) OVER (
                    PARTITION BY badge_id
                    ORDER BY event_time
                ) AS next_event_time,
                LEAD(event_type) OVER (
                    PARTITION BY badge_id
                    ORDER BY event_time
                ) AS next_event_type
            FROM src_badge_events
            -- include previous day to capture overnight sessions
            WHERE event_time >= '{{ ds }}'::date - INTERVAL '1 day'
              AND event_time <  '{{ ds }}'::date + INTERVAL '1 day'
        ),

        paired_sessions AS (
            SELECT
                badge_id,
                event_time AS in_time,
                next_event_time AS out_time
            FROM ordered_events
            WHERE event_type = 'IN'
              AND next_event_type = 'OUT'
              AND next_event_time IS NOT NULL
        ),

        expanded_days AS (
            SELECT
                badge_id,
                generate_series(
                    DATE(in_time),
                    DATE(out_time),
                    INTERVAL '1 day'
                )::date AS date_id,
                in_time,
                out_time
            FROM paired_sessions
        ),

        daily_slices AS (
            SELECT
                badge_id,
                date_id,
                GREATEST(in_time, date_id::timestamp) AS day_in,
                LEAST(out_time, date_id::timestamp + INTERVAL '1 day') AS day_out
            FROM expanded_days
        ),

        daily_sessions AS (
            SELECT
                badge_id,
                date_id,
                day_in,
                day_out,
                EXTRACT(EPOCH FROM (day_out - day_in)) / 3600 AS session_hours
            FROM daily_slices
            WHERE day_out > day_in
        ),

        daily_aggregates AS (
            SELECT
                badge_id,
                date_id,
                SUM(session_hours)                    AS total_hours,
                COUNT(*)                              AS total_sessions,
                MIN(day_in)                           AS first_in_time,
                MAX(day_out)                          AS last_out_time,
                AVG(session_hours)                    AS avg_session_hours
            FROM daily_sessions
            GROUP BY badge_id, date_id
        )

        INSERT INTO fact_attendance_daily (
            badge_id,
            date_id,
            total_hours,
            total_sessions,
            first_in_time,
            last_out_time,
            avg_session_hours
        )
        SELECT
            badge_id,
            date_id,
            total_hours,
            total_sessions,
            first_in_time,
            last_out_time,
            avg_session_hours
        FROM daily_aggregates
        WHERE date_id = '{{ ds }}'::date;
        """
    )

    create_fact_table >> calculate_daily_attendance
