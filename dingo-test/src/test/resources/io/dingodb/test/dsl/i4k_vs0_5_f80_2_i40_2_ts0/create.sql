CREATE TABLE {table} (
    employee_id int NOT NULL,
    first_name varchar(20) DEFAULT NULL,
    last_name varchar(25) DEFAULT NULL,
    email varchar(25) DEFAULT NULL,
    phone_number varchar(20) DEFAULT NULL,
    job_id varchar(10) DEFAULT NULL,
    salary double DEFAULT NULL,
    commission_pct double DEFAULT NULL,
    manager_id int DEFAULT NULL,
    department_id int DEFAULT NULL,
    hire_date timestamp DEFAULT NULL,
    PRIMARY KEY (employee_id)
)
