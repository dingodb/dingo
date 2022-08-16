select beauties.*, boys.* from beauties
right join boys on boys.id = beauties.boyfriend_id
where beauties.id < 10
