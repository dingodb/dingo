select
    s.id,
    s.title,
    a.name as artistName,
    a.category as artistCategory
from {table0} s
left join {table1} a on s.artist = a.id
where a.id is not null
