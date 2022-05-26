select
    e.employee_id,
    e.first_name,
    e.last_name,
    e.hire_date,
    j.job_id,
    j.job_title
from employees e
join jobs j using(job_id)
