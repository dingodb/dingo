select
    s.id,
    s.title,
    a.id as artistId,
    a.name as artistName
from {table0} s
join {table1} a on s.artist <> a.id
