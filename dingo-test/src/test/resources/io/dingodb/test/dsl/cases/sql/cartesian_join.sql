select
    s.id,
    s.title,
    a.id as artistId,
    a.name as artistName
from {table0} s, {table1} a
