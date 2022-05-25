select
    s.id,
    s.title,
    a.id as artistId,
    a.name as artistName
from songs s
join artists a on s.artist <> a.id
