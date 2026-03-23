# 📊 Student & Teacher Attendance Analytics Pipeline

## 📌 Overview

An end-to-end **data engineering and analytics pipeline** that simulates campus badge swipe data and transforms it into actionable attendance insights.

The system ingests raw events, builds a **star schema warehouse**, and generates KPIs using **Airflow-orchestrated ETL pipelines** and SQL-based analytics.

---

## 🚀 What This Project Does

* Simulates badge swipe data for students and teachers
* Builds a **star schema data warehouse**
* Automates ETL using **Apache Airflow DAGs**
* Computes daily attendance metrics (hours, sessions, trends)
* Enables analysis via SQL + interactive visualizations

---

## 🛠️ Tech Stack

* Python
* Apache Airflow
* PostgreSQL / SQLite
* Pandas
* Plotly
* Jupyter Notebook

---

## ⚙️ Pipeline (High Level)

```
Raw Events → Airflow ETL → Fact Table → SQL Analysis → Visualizations
```

* Raw badge events stored in `src_badge_events`
* Transformed into `fact_attendance_daily`
* Joined with dimension tables (`students`, `teachers`, `date`)
* Used for KPI analysis and dashboards

---

## 🧱 Data Model

* **Fact Table:** `fact_attendance_daily`
* **Dimensions:** `dim_students`, `dim_teachers`, `dim_date`

Each row represents:

> one person × one day → attendance metrics (hours, sessions, first IN, last OUT)

---

## 🔄 Airflow Pipelines

* **Dimension DAG** → creates & seeds students, teachers, calendar
* **Source DAG (Daily)** → simulates badge swipe events
* **Daily Fact DAG** → computes attendance per day
* **Bulk Fact DAG (Weekly)** → handles backfill + cross-day sessions

✔ Handles:

* IN → OUT session pairing using SQL window functions
* Cross-midnight sessions
* Missing OUT events
* Idempotent re-runs

---

## 📊 Key Insights Enabled

* Attendance trends over time
* Department-wise engagement
* Top / low performers
* Student vs Teacher behavior patterns

---

## 📂 Project Structure

```
airflow/dags/        → ETL pipelines  
snapshots/           → notebooks + analysis  
```

---

## ▶️ How to Run

### Airflow

```
airflow standalone
```

Trigger:

1. `source_data_dim_dag`
2. `source_data_src_dag`
3. `fact_attendance_daily_dag` / bulk

### Notebook

```
jupyter notebook snapshots/
```

---

## 💡 Key Highlights

* Built **idempotent ETL pipelines** (safe re-runs)
* Used **window functions (LEAD)** for session logic
* Designed **analytical star schema**
* Handled real-world edge cases (missing events, cross-day sessions)

---

## 🧠 What This Shows

This project demonstrates:

* Data Engineering fundamentals
* Workflow orchestration (Airflow)
* SQL-based analytics
* End-to-end pipeline thinking

---

## 👩‍💻 Author

**Ananya B S**
GitHub: https://github.com/Ananya102005
