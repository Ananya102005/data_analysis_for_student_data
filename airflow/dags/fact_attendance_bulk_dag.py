from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState
from datetime import datetime, timedelta

with DAG(
    dag_id="fact_attendance_bulk_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@weekly",
    catchup=True,
    tags=["fact", "attendance", "bulk"]
) as dag:

    wait_for_src_dag = ExternalTaskSensor(
        task_id="wait_for_source_data_src_dag",
        external_dag_id="source_data_src_dag",
        external_task_id=None,
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        mode="reschedule",
        timeout=60 * 60 * 2,
        poke_interval=60,
        execution_delta=timedelta(days=6),
    )

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

    calculate_bulk_attendance = PostgresOperator(
        task_id="calculate_bulk_attendance",
        postgres_conn_id="postgres_default",
        sql="""
        -- Bulk idempotent delete for entire week
        DELETE FROM fact_attendance_daily
        WHERE date_id >= '{{ ds }}'::date
          AND date_id < '{{ ds }}'::date + INTERVAL '7 days';

        WITH stage_data AS (
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
        ),

        daily_calculation AS (
            SELECT
                d.date_id,
                s.badge_id,

                CASE
                    WHEN s.badge_id IS NULL THEN INTERVAL '0 seconds'
                    ELSE
                        (
                            CASE
                                WHEN s.next_event_time IS NULL
                                    OR s.next_event_type <> 'OUT'
                                THEN LEAST(
                                    DATE_TRUNC('day', s.event_time) + INTERVAL '1 day',
                                    d.date_id + INTERVAL '1 day'
                                )
                                WHEN s.next_event_time > d.date_id + INTERVAL '1 day'
                                THEN d.date_id + INTERVAL '1 day'
                                ELSE s.next_event_time
                            END
                            -
                            CASE
                                WHEN s.event_time < d.date_id
                                THEN d.date_id::timestamp
                                ELSE s.event_time
                            END
                        )
                END AS session_duration,

                CASE
                    WHEN s.badge_id IS NULL THEN NULL
                    WHEN s.event_time < d.date_id THEN d.date_id::timestamp
                    ELSE s.event_time
                END AS effective_in,

                CASE
                    WHEN s.badge_id IS NULL THEN NULL
                    WHEN s.next_event_time IS NULL
                        OR s.next_event_type <> 'OUT'
                    THEN LEAST(
                        DATE_TRUNC('day', s.event_time) + INTERVAL '1 day',
                        d.date_id + INTERVAL '1 day'
                    )
                    WHEN s.next_event_time > d.date_id + INTERVAL '1 day'
                    THEN d.date_id + INTERVAL '1 day'
                    ELSE s.next_event_time
                END AS effective_out

            FROM dim_date d
            LEFT JOIN stage_data s
                ON date_trunc('day', s.event_time) <= d.date_id
                AND (
                    -- Incomplete session: only on the day it started
                    (
                        (s.next_event_time IS NULL OR s.next_event_type <> 'OUT')
                        AND DATE_TRUNC('day', s.event_time) = d.date_id
                    )
                    OR
                    -- Complete session: can span multiple days
                    date_trunc('day', s.next_event_time) >= d.date_id
                )
                AND s.event_type = 'IN'
            WHERE d.date_id >= '{{ ds }}'::date
              AND d.date_id < '{{ ds }}'::date + INTERVAL '7 days'
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
            COALESCE(badge_id, 'NO_ATTENDANCE') AS badge_id,
            date_id,
            COALESCE(
                SUM(EXTRACT(EPOCH FROM session_duration) / 3600), 0
            ) AS total_hours,
            COUNT(
                CASE WHEN session_duration > INTERVAL '0 seconds' THEN 1 END
            ) AS total_sessions,
            MIN(effective_in) AS first_in_time,
            MAX(effective_out) AS last_out_time,
            COALESCE(
                AVG(NULLIF(EXTRACT(EPOCH FROM session_duration) / 3600, 0)), 0
            ) AS avg_session_hours
        FROM daily_calculation
        GROUP BY COALESCE(badge_id, 'NO_ATTENDANCE'), date_id;
        """
    )

    wait_for_src_dag >> create_fact_table >> calculate_bulk_attendance
