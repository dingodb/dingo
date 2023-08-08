select
    s.id,
    s.title,
    a.name as artistName,
    a.category as artistCategory
from {table0} s
join {table1} a on s.artist = a.id
where a.category = 'Band'
