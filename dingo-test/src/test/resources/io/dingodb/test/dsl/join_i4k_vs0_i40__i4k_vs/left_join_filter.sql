select
    {table0}.*,
    {table1}.*
from {table0}
left join {table1} on {table0}.class_id = {table1}.cid
where {table0}.sid = 1
