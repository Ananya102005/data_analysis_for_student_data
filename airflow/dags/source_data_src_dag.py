from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="source_data_src_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=True,
    tags=["source", "badge"]
) as dag:

    # ------------------------------------------------------------------
    # Create source table
    # ------------------------------------------------------------------
    create_badge_events_table = PostgresOperator(
        task_id="create_badge_events_table",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS src_badge_events (
            event_id SERIAL PRIMARY KEY,
            badge_id TEXT NOT NULL,
            event_time TIMESTAMP NOT NULL,
            event_type TEXT CHECK (event_type IN ('IN','OUT'))
        );
        """
    )

    # ------------------------------------------------------------------
    # Insert STUDENT badge events
    # ------------------------------------------------------------------
    insert_student_events = PostgresOperator(
        task_id="insert_student_events",
        postgres_conn_id="postgres_default",
        sql="""
        WITH day_context AS (
            SELECT
                EXTRACT(DOW FROM '{{ ds }}'::date) AS dow,
                CASE
                    WHEN EXTRACT(DOW FROM '{{ ds }}'::date) = 0
                        THEN 1.0                              -- Sunday holiday
                    WHEN EXTRACT(DOW FROM '{{ ds }}'::date) = 6
                        THEN 0.40 + random() * 0.20           -- Saturday
                    ELSE
                        0.03 + random() * 0.10                -- Weekday
                END AS absentee_rate,
                CASE
                    WHEN EXTRACT(DOW FROM '{{ ds }}'::date) IN (0,6)
                        THEN 0.10
                    ELSE
                        0.25
                END AS revisit_prob
        )
        INSERT INTO src_badge_events (badge_id, event_time, event_type)
        SELECT
            s.badge_id,
            '{{ ds }}'::date + ev.event_time,
            ev.event_type
        FROM students s
        CROSS JOIN day_context d
        CROSS JOIN LATERAL (
            SELECT
                CASE
                    WHEN random() < d.revisit_prob THEN 2
                    ELSE 1
                END AS visits
        ) v
        CROSS JOIN LATERAL (
            SELECT
                time '00:00' + random() * interval '24 hours' AS in_time,
                LEAST(
                    time '00:00'
                        + random() * interval '24 hours'
                        + (interval '30 minutes' + random() * interval '10 hours'),
                    time '23:59'
                ) AS out_time
            FROM generate_series(1, v.visits)
        ) p
        CROSS JOIN LATERAL (
            SELECT p.in_time AS event_time, 'IN' AS event_type
            UNION ALL
            SELECT p.out_time, 'OUT'
        ) ev
        WHERE d.dow <> 0
          AND random() > d.absentee_rate;
        """
    )

    # ------------------------------------------------------------------
    # Insert TEACHER badge events
    # ------------------------------------------------------------------
    insert_teacher_events = PostgresOperator(
        task_id="insert_teacher_events",
        postgres_conn_id="postgres_default",
        sql="""
        WITH day_context AS (
            SELECT
                EXTRACT(DOW FROM '{{ ds }}'::date) AS dow,
                CASE
                    WHEN EXTRACT(DOW FROM '{{ ds }}'::date) = 0
                        THEN 1.0                              -- Sunday holiday
                    WHEN EXTRACT(DOW FROM '{{ ds }}'::date) = 6
                        THEN 0.25 + random() * 0.15           -- Saturday
                    ELSE
                        0.01 + random() * 0.04                -- Weekday
                END AS absentee_rate
        )
        INSERT INTO src_badge_events (badge_id, event_time, event_type)
        SELECT
            t.badge_id,
            '{{ ds }}'::date + ev.event_time,
            ev.event_type
        FROM teachers t
        CROSS JOIN day_context d
        CROSS JOIN LATERAL (
            SELECT
                time '00:00' + random() * interval '24 hours' AS in_time,
                LEAST(
                    time '00:00'
                        + random() * interval '24 hours'
                        + (interval '2 hours' + random() * interval '8 hours'),
                    time '23:59'
                ) AS out_time
        ) p
        CROSS JOIN LATERAL (
            SELECT p.in_time AS event_time, 'IN' AS event_type
            UNION ALL
            SELECT p.out_time, 'OUT'
        ) ev
        WHERE d.dow <> 0
          AND random() > d.absentee_rate;
        """
    )

    # ------------------------------------------------------------------
    # DAG order
    # ------------------------------------------------------------------
    create_badge_events_table >> insert_student_events >> insert_teacher_events
