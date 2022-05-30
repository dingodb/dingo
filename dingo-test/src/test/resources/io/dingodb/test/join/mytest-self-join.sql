select
    a.name aname,
    b.name bname
from mytest a
inner join mytest b on a.manager_id = b.id
