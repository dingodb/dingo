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

### Prepare tables

```sql
-- table `permission`
CREATE TABLE permission (
    create_time BIGINT NOT NULL,
    update_time BIGINT NOT NULL,
    type VARCHAR(255) NOT NULL,
    operation_id INTEGER NOT NULL,
    resource_id INTEGER NOT NULL,
    remark VARCHAR(255),
    status INTEGER NOT NULL,
    id INTEGER NOT NULL AUTO_INCREMENT,
    PRIMARY KEY (id)
) AUTO_INCREMENT = 1000 COMMENT='权限信息表';
INSERT INTO permission (CREATE_TIME,UPDATE_TIME,`TYPE`,OPERATION_ID,RESOURCE_ID,REMARK,STATUS,ID) VALUES
(1702282318591,1702282318591,'menu',1,1,NULL,1,1),
(1702282318591,1702282318591,'menu',1,2,NULL,1,2),
(1702282318591,1702282318591,'menu',1,3,NULL,1,3),
(1702282318591,1702282318591,'menu',1,4,NULL,1,4),
(1702282318591,1702282318591,'menu',1,5,NULL,1,5),
(1702282318591,1702282318591,'menu',1,6,NULL,1,6),
(1702282318591,1702282318591,'element',2,7,NULL,1,7),
(1702282318591,1702282318591,'element',2,8,NULL,1,8),
(1702282318591,1702282318591,'element',2,9,NULL,1,9),
(1702282318591,1702282318591,'element',2,10,NULL,1,10);

-- table `resource`
CREATE TABLE resource (
    create_time BIGINT NOT NULL,
    update_time BIGINT NOT NULL,
    id INTEGER NOT NULL AUTO_INCREMENT,
    parent_id INTEGER,
    name VARCHAR(255),
    type VARCHAR(255) NOT NULL,
    rel_id INTEGER,
    `key` VARCHAR(255),
    remark VARCHAR(255),
    status INTEGER NOT NULL,
    PRIMARY KEY (id)
) AUTO_INCREMENT = 1000 COMMENT='资源信息表';
INSERT INTO resource (CREATE_TIME,UPDATE_TIME,ID,PARENT_ID,NAME,`TYPE`,REL_ID,`KEY`,REMARK,STATUS) VALUES
(1702282318591,1702282318591,1,0,'智问智答','menu',NULL,'chat',NULL,1),
(1702282318591,1702282318591,2,0,'企业应用','menu',NULL,'app',NULL,1),
(1702282318591,1702282318591,3,0,'数据管理','menu',NULL,'data',NULL,1),
(1702282318591,1702282318591,4,0,'模型微调','menu',NULL,'model',NULL,1),
(1702282318591,1702282318591,5,0,'构建工作流','menu',NULL,'buildApp',NULL,1),
(1702282318591,1702282318591,6,0,'系统管理','menu',NULL,'system',NULL,1),
(1702282318591,1702282318591,7,1,'创意抽象资源创建','element',NULL,'creativity_publish',NULL,1),
(1702282318591,1702282318591,8,1,'编辑创意','element',NULL,'creativity_edit',NULL,1),
(1702282318591,1702282318591,9,1,'删除创意','element',NULL,'creativity_delete',NULL,1),
(1702282318591,1702282318591,10,2,'新建应用','element',NULL,'app_create',NULL,1);

-- table `operation`
CREATE TABLE operation (
    create_time BIGINT NOT NULL,
    update_time BIGINT NOT NULL,
    name VARCHAR(255) NOT NULL,
    code VARCHAR(255) NOT NULL,
    type VARCHAR(255) NOT NULL,
    remark VARCHAR(255),
    id INTEGER NOT NULL AUTO_INCREMENT,
    PRIMARY KEY (id)
) AUTO_INCREMENT = 1000 COMMENT='功能操作表';
INSERT INTO operation (CREATE_TIME,UPDATE_TIME,NAME,CODE,`TYPE`,REMARK,ID) VALUES
(1702282318591,1702282318591,'查看菜单','view','menu',NULL,1),
(1702282318591,1702282318591,'查看按钮','view','element',NULL,2),
(1702282318591,1702282318591,'所有权','own','dataset',NULL,3),
(1702282318591,1702282318591,'所有权','own','creativity',NULL,4),
(1702282318591,1702282318591,'所有权','own','app',NULL,5);
```

### Join 3 tables

```sql
select p.id pid, r.id rid, r.name rname, r.`type`, r.`key`, o.code, o.name oname, r.rel_id from permission as p
left join resource as r on r.id = p.RESOURCE_ID and r.parent_id = 6
left join operation as o on o.id = p.OPERATION_ID
where r.id in (1, 3, 6, 8, 9, 21, 22, 23, 24, 33, 34, 35, 36, 37, 38, 39, 40, 66, 67, 68);
```
