CREATE DATABASE IF NOT EXISTS recruitment;

CREATE TABLE recruitment.recruitment_all_report (
    title STRING,
    last_updated TIMESTAMP,
    created_time TIMESTAMP,
    skills ARRAY<STRING>,
    requirement ARRAY<STRING>,
    hirer STRING,
    company_link STRING,
    company_location STRING,
    category ARRAY<STRING>,
    min_salary INT,
    max_salary INT,
    salary_range STRING,
    location STRING,
    work_type STRING,
    work_time_type STRING,
    company_name STRING
)
PARTITIONED BY (year INT, month INT, day INT)
STORED AS PARQUET;