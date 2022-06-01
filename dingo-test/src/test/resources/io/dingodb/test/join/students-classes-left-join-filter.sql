select
    students.*,
    classes.*
from students
left join classes on students.class_id = classes.cid
where students.sid = 1
