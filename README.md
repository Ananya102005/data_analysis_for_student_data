# 📊 Data Analysis for Student Attendance

## Overview

This project is a complete data engineering and analytics workflow that simulates and analyzes student and teacher attendance using badge IN/OUT events.

It demonstrates how raw event-level data can be transformed into structured analytical insights using SQL, star schema modeling, and interactive visualizations.

The project is implemented fully using Python, SQLite, and Jupyter Notebooks.

---

## Project Objective

The main goals of this project are:

- To simulate realistic badge swipe data for students and teachers  
- To design and implement a structured analytical database  
- To transform raw events into meaningful fact tables  
- To perform SQL-based analytics  
- To generate visual insights using Plotly  

---

## Technology Stack

- **Database:** SQLite  
- **Programming Language:** Python  
- **Libraries Used:**
  - pandas  
  - faker  
  - plotly  
  - sqlite3  
- **Environment:** Jupyter Notebook  
- **Version Control:** GitHub  

---

## Database Design

The project follows **star schema architecture**, which is an industry-standard design for analytical systems.

### Source Table

**src_badge_events**

This table contains raw simulated badge swipe events.

Columns:

- badge_id  
- event_time  
- event_type (IN / OUT)  

This acts as the transactional data layer.

---

### Dimension Tables

Three main dimension tables provide context to the fact data:

#### dim_students
Contains student master information.

- student_id  
- badge_id  
- name  
- department  
- year_or_sem  

#### dim_teachers
Contains teacher master information.

- teacher_id  
- badge_id  
- name  
- department  

#### dim_calendar
A standard industry calendar dimension for time-based analytics.

- date_id  
- full_date  
- day  
- month  
- year  
- weekday  
- is_weekend  

---

### Fact Table

**fact_attendance_daily**

This is the central analytical table.

It aggregates raw badge events into daily attendance metrics.

Columns:

- badge_id  
- date_id  
- total_hours  
- total_sessions  
- first_in_time  
- last_out_time  
- avg_session_hours  

The fact table is designed to be slim and metric-focused, following best practices of data warehousing.

---

## Data Flow

The overall pipeline works as follows:

1. Synthetic attendance data is generated using Python and Faker  
2. Data is stored in `src_badge_events`  
3. Dimension tables are created  
4. Raw events are aggregated into `fact_attendance_daily`  
5. SQL analytics are performed  
6. Visual insights are generated using Plotly  

---

## Analytics Performed

Using the fact and dimension tables, the following analyses were implemented:

- Month-over-Month attendance trends  
- Top 10 students by attendance  
- Top 10 teachers by attendance  
- Subject-wise attendance distribution  
- Attendance patterns by weekday  
- Comparison between student and teacher attendance  

---

## Visualization

Interactive visualizations were created using Plotly, including:

- Line charts for attendance trends  
- Bar charts for top performers  
- Pie charts for subject-wise distribution  
- Comparative analysis charts  

These dashboards help convert raw data into meaningful insights.

---

