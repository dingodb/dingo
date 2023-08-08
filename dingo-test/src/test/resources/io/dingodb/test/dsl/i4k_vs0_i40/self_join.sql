select
    a.name aname,
    b.name bname
from {table} a
inner join {table} b on a.manager_id = b.id
