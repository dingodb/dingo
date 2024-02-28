select {table0}.*, {table1}.class_no, {table2}.score from {table0}
inner join {table1} on {table1}.class_no={table0}.class_no
left join {table2} on {table2}.sno={table0}.sno where {table2}.sno is not null
