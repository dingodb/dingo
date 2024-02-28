select {table0}.*, {table1}.* from {table0}
right join {table1} on {table1}.id = {table0}.boyfriend_id
where {table0}.id < 10
