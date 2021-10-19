# Quick Start about DingoDB

We want your experience getting started with DingoDB to be both low effort and high reward. Here you'll find a collection of quick start guides about the basic operations about dingo such as DDL and DML using SQL. This tutorial will guide you through creating a DingoDB `Table`, insert a sample dataset, and then executing queries on that data. This will take around 10 minutes to complete

## Start Dingo Cluster 

### Option-1: Using Github docker registry

```shell
curl -sSL https://raw.githubusercontent.com/dingodb/dingo/main/docker/docker-compose.yml?token=ABUTRTNQUWN3TGXJ5SE3MTLBM7NT6 > docker-compose.yml
docker-compose up -d
docker exec -it `docker ps | grep executor | awk '{print $1}'` /bin/bash
./bin/sqlline.sh
```

> Prerequisites: A personal access token[PAT](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token) has been created and the authentication has been granted to [github container registry](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry). 

### Option-2: Using Docker hub registry

- Download docker-compose file

```shell
curl -sSL https://raw.githubusercontent.com/dingodb/dingo/main/docker/docker-compose.yml?token=ABUTRTNQUWN3TGXJ5SE3MTLBM7NT6 > docker-compose.yml
```

- Change docker image registry

```shell
sed -i 's/ghcr.io\/dingodb\/dingo-ubuntu:main/dingodatabase\/dingodb:0.1.0/g' docker-compose.yml
```

- Start Dingo cluster

```
docker-compose up -d
docker exec -it `docker ps | grep executor | awk '{print $1}'` /bin/bash
./bin/sqlline.sh
```


## Import Dataset

1. Create Table

```sql
create table userinfo (
    id int,
    name varchar(32) not null,
    age int,
    amount double,
    primary key(id)
);
```

2. Import Dataset to Table

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

> You use view the table structure using such command `!describe userinfo`
  

## Execute a Simple Query

1. Query all data of `userinfo`

```sql
select * from userinfo;
```

2. Query table by condition

```sql
select * from userinfo where id=3;
```

3. Query table by aggregation condition

```sql
select name, sum(amount) as sumAmount from userinfo group by name;
```

4. Query table by `order by`

```sql
select * from userinfo order by id;
```

5. Update table by row or condition

```sql
update userinfo set age = age + 10;
update userinfo set age = age + 10 where id = 1;
```
