create table t_auto(id int auto_increment, name varchar(32), age int, primary key (id)) partition by range values (2),(3);
