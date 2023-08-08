select
    s.id,
    s.title,
    a.name as artistName,
    a.category as artistCategory
from {table0} s
full join {table1} a on s.artist = a.id
