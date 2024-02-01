# Case Analyzing

## Join 3 tables

### Prepare tables

```sql
-- table `student`
drop table if exists student;
CREATE TABLE student (
    sno varchar(6),
    sname varchar(20),
    sage int,
    ssex varchar(10),
    class_no varchar(6),
    primary key(sno)
);
insert into student values
('1001', 'Li Bai', 15, 'Male', '901'),
('1002', 'Du Fu', 15, 'Male', '902'),
('1003', 'Bai Juyi', 13, 'Female','902'),
('1004', 'Tian E', 18, 'Female','903'),
('1005', 'Wang Bo', 20, 'Male','904'),
('1006', 'Li Qingzhao', 33, 'Female', '903'),
('1007', 'Waner', 21, 'Female', '905'),
('1008', 'Li Yu', 47, 'Male', '905');

-- table `score`
drop table if exists score;
CREATE TABLE score (
    sno varchar(6),
    cno varchar(6),
    score int,
    primary key(sno,cno)
);
insert into score values
('1001', '100', 60),
('1001', '101', 60),
('1001', '102', 80),
('1002', '100', 95),
('1002', '101', 75),
('1002', '102', 100),
('1003', '100', 59),
('1003', '101', 78),
('1003', '102', 91),
('1004', '100', 88),
('1004', '101', 50);

-- table `courses`
drop table if exists courses;
CREATE TABLE courses (
    cno varchar(6),
    cname varchar(20),
    tno varchar(10),
    primary key (cno)
);
insert into courses values
('100', 'Math', '200'),
('101', 'Language', '201'),
('102', 'English', '202');

-- table `classes`
drop table if exists classes;
create table classes (
    class_no varchar(6),
    class_name varchar(12),
    primary key(class_no)
);
insert into classes values
('901','class1-1'),
('902','class1-2'),
('903','class1-3'),
('904','class1-4'),
('906','class1-6');
```

### Join 3 tables with sub query

```sql
select a.sno, a.sname, a.cname from (
    select st.sno, st.sname, c.cno, c.cname from student st cross join courses c where sno < '1005'
) a
left join (
    select st.sno, st.sname, sc.cno from student st inner join score sc on sc.sno = st.sno
) b on a.sno = b.sno and a.cno = b.cno where b.cno is null;
```

### Join 3 tables

```sql
select student.*, classes.class_no, score.score from student
inner join classes on classes.class_no=student.class_no
left join score on score.sno=student.sno where score.sno is not null;
```
