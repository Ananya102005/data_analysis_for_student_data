from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState
from datetime import datetime

with DAG(
    dag_id="fact_attendance_daily_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=True,
    tags=["fact", "attendance"]
) as dag:

    # ------------------------------------------------------------
    # Wait for SRC DAG (DAG-to-DAG dependency)
    # ------------------------------------------------------------
    wait_for_src_dag = ExternalTaskSensor(
        task_id="wait_for_source_data_src_dag",
        external_dag_id="source_data_src_dag",
        external_task_id=None,          # wait for entire DAG
        allowed_states=[DagRunState.SUCCESS],
        failed_states=[DagRunState.FAILED],
        mode="reschedule",
        timeout=60 * 60 * 2,            # 2 hours
        poke_interval=60,
    )

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
    # Populate FACT table 
    # ------------------------------------------------------------
    calculate_daily_attendance = PostgresOperator(
        task_id="calculate_daily_attendance",
        postgres_conn_id="postgres_default",
        sql="""
        -- Idempotent reload
        DELETE FROM fact_attendance_daily
        WHERE date_id = '{{ ds }}'::date;

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
                badge_id,
                '{{ ds }}'::date AS date_id,

                (
                    CASE
                        WHEN next_event_time > '{{ ds }}'::date + INTERVAL '1 day'
                        THEN '{{ ds }}'::date + INTERVAL '1 day'
                        ELSE next_event_time
                    END
                    -
                    CASE
                        WHEN event_time < '{{ ds }}'::date
                        THEN '{{ ds }}'::date
                        ELSE event_time
                    END
                ) AS session_duration,

                CASE
                    WHEN event_time < '{{ ds }}'::date
                    THEN '{{ ds }}'::date
                    ELSE event_time
                END AS effective_in,

                CASE
                    WHEN next_event_time > '{{ ds }}'::date + INTERVAL '1 day'
                    THEN '{{ ds }}'::date + INTERVAL '1 day'
                    ELSE next_event_time
                END AS effective_out

            FROM stage_data
            WHERE event_type = 'IN'
              AND next_event_type = 'OUT'
              AND next_event_time IS NOT NULL
              AND date_trunc('day', event_time) <= '{{ ds }}'::date
              AND date_trunc('day', next_event_time) >= '{{ ds }}'::date
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
            SUM(EXTRACT(EPOCH FROM session_duration) / 3600) AS total_hours,
            COUNT(*)                                        AS total_sessions,
            MIN(effective_in)                               AS first_in_time,
            MAX(effective_out)                              AS last_out_time,
            AVG(EXTRACT(EPOCH FROM session_duration) / 3600) AS avg_session_hours
        FROM daily_calculation
        WHERE session_duration > INTERVAL '0 seconds'
        GROUP BY badge_id, date_id;
        """
    )

    # ------------------------------------------------------------
    # DAG order
    # ------------------------------------------------------------
    wait_for_src_dag >> create_fact_table >> calculate_daily_attendance
