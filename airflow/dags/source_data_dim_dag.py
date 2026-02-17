from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="source_data_dim_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["source", "dimension"]
) as dag:

    create_dim_tables = PostgresOperator(
        task_id="create_dim_tables",
        postgres_conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS students (
            student_id SERIAL PRIMARY KEY,
            badge_id TEXT UNIQUE NOT NULL,
            name TEXT,
            department TEXT,
            year_or_sem TEXT
        );

        CREATE TABLE IF NOT EXISTS teachers (
            teacher_id SERIAL PRIMARY KEY,
            badge_id TEXT UNIQUE NOT NULL,
            name TEXT,
            department TEXT
        );

        CREATE TABLE IF NOT EXISTS dim_date (
            date_id DATE PRIMARY KEY,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            day_of_week INTEGER,
            week_of_year INTEGER,
            is_weekend BOOLEAN,
            is_holiday BOOLEAN DEFAULT FALSE
        );
        """
    )

    load_dim_date = PostgresOperator(
        task_id="load_dim_date",
        postgres_conn_id="postgres_default",
        sql="""
        INSERT INTO dim_date (
            date_id,
            year,
            month,
            day,
            day_of_week,
            week_of_year,
            is_weekend
        )
        SELECT
            date_val::date,
            EXTRACT(YEAR FROM date_val)::INTEGER,
            EXTRACT(MONTH FROM date_val)::INTEGER,
            EXTRACT(DAY FROM date_val)::INTEGER,
            EXTRACT(DOW FROM date_val)::INTEGER,
            EXTRACT(WEEK FROM date_val)::INTEGER,
            EXTRACT(DOW FROM date_val) IN (0, 6)
        FROM generate_series(
            '2026-01-01'::date,
            '2027-12-31'::date,
            '1 day'::interval
        ) date_val
        ON CONFLICT (date_id) DO NOTHING;
        """
    )

    load_students = PostgresOperator(
        task_id="load_students",
        postgres_conn_id="postgres_default",
        sql="""
        INSERT INTO students (badge_id, name, department, year_or_sem)
        SELECT
            'BADGE' || LPAD(gs::text, 6, '0'),
            'Student_' || gs,
            CASE
                WHEN gs % 6 = 0 THEN 'Arts'
                WHEN gs % 6 = 1 THEN 'Business'
                WHEN gs % 6 = 2 THEN 'Engineering'
                WHEN gs % 6 = 3 THEN 'Law'
                WHEN gs % 6 = 4 THEN 'Medicine'
                ELSE 'Science'
            END,
            ((gs % 4) + 1)::text
        FROM generate_series(1, 2000) gs
        ON CONFLICT (badge_id) DO NOTHING;
        """
    )

    load_teachers = PostgresOperator(
        task_id="load_teachers",
        postgres_conn_id="postgres_default",
        sql="""
        INSERT INTO teachers (badge_id, name, department)
        SELECT
            'TEACHER' || LPAD(gs::text, 5, '0'),
            'Teacher_' || gs,
            CASE
                WHEN gs % 6 = 0 THEN 'Arts'
                WHEN gs % 6 = 1 THEN 'Business'
                WHEN gs % 6 = 2 THEN 'Engineering'
                WHEN gs % 6 = 3 THEN 'Law'
                WHEN gs % 6 = 4 THEN 'Medicine'
                ELSE 'Science'
            END
        FROM generate_series(1, 150) gs
        ON CONFLICT (badge_id) DO NOTHING;
        """
    )

    create_dim_tables >> load_dim_date >> [load_students, load_teachers]