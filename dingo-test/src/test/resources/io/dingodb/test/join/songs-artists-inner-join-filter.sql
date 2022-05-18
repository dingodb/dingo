select
    s.id,
    s.title,
    a.name as artistName,
    a.category as artistCategory
from songs s
join artists a on s.artist = a.id
where a.category = 'Band'
