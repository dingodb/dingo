select
    students.*,
    classes.*
from students
right join classes on students.class_id = classes.cid
where students.sid = 1
