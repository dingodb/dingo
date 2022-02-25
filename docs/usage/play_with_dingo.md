# Play with DingoDB using SQL

## Import Dataset

### Create Table

```sql
create table userinfo (
    id int,
    name varchar(32) not null,
    age int,
    amount double,
    primary key(id)
);
```

### Insert values to Table

```sql
insert into userinfo values
(1, 'Alice', 18, 3.5),
(2, 'Betty', 22, 4.1),
(3, 'Cindy', 39, 4.6),
(4, 'Doris', 25, 5.2),
(5, 'Emily', 24, 5.8),
(6, 'Alice', 32, 6.1),
(7, 'Betty', 30, 6.9),
(8, 'Alice', 22, 7.3),
(9, 'Cindy', 26, 7.5);
```

## Execute Query

- Query all data of `userinfo`

```sql
select * from userinfo;
```

- Query table by condition

```sql
select * from userinfo where id=3;
```

- Query table by aggregation condition

```sql
select name, sum(amount) as sumAmount from userinfo group by name;
```

- Query table by `order by`

```sql
select * from userinfo order by id;
```

- Update table by row or condition

```sql
update userinfo set age = age + 10;
update userinfo set age = age + 10 where id = 1;
```
