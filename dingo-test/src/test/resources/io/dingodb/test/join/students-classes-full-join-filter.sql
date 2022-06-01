select
    students.*,
    classes.*
from students
full join classes on students.class_id = classes.cid
where students.sid = 1
