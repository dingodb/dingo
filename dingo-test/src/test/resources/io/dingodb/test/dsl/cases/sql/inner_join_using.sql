select
    e.employee_id,
    e.first_name,
    e.last_name,
    e.hire_date,
    j.job_id,
    j.job_title
from {table0} e
join {table1} j using(job_id)
