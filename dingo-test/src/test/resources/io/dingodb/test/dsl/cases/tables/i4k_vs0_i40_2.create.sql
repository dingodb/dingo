CREATE TABLE {table} (
    job_id varchar(10) NOT NULL,
    job_title varchar(50) DEFAULT NULL,
    min_salary int DEFAULT NULL,
    max_salary int DEFAULT NULL,
    PRIMARY KEY (job_id)
)
