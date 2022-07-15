create table non_scalar (
    id int,
    anyCol any,
    intArrayCol int array,
    intMultiSetCol int multiset,
    rowCol row(a int, b varchar(10)),
    primary key(id)
)
