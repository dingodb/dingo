select p.id pid, r.id rid, r.name rname, r.`type`, r.`key`, o.code, o.name oname, r.rel_id from DINGO.{table0} as p
left join DINGO.{table1} as r on r.id = p.RESOURCE_ID and r.parent_id = 6
left join DINGO.{table2} as o on o.id = p.OPERATION_ID
where r.id in (1, 3, 6, 7, 8, 9, 21, 22, 23, 24, 33, 34, 35, 36, 37, 38, 39, 40, 66, 67, 68)
