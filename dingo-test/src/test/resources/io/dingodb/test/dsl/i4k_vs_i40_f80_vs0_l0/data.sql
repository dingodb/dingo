--- Value must be the same type in one insert statement, so split them.
insert into {table} values
(1, 'c1', 28, 109.325, 'beijing', true),
(2, 'c2', 17, 139.2, 'beijing', false);
insert into {table} values
(3, 'c3', 22, 34.89, 'shanghai', 0),
(4, 'c4', 33, 3392.88, 'zhengzhou', 1),
(5, 'c5', 39, 342.01, 'beijing', 100),
(6, 'c6', 11, 3.6, 'zhengzhou', 0);
insert into {table} values
(7, 'c7', 19, 223.18, 'shanghai', null),
(8, 'c8', 29, 2.1, 'shanghai', null)
