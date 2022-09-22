select beauties.*, boys.* from beauties
right join boys on boys.id = beauties.boyfriend_id
where boys.boyName not in ('Han Han', 'Duan Yu')
