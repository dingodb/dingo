create table {table} (
    id int NOT NULL,
    name varchar(50) NOT NULL,
    sex varchar(10) DEFAULT 'female',
    born_date timestamp DEFAULT '1987-01-01 00:00:00',
    phone varchar(11) NOT NULL,
    boyfriend_id int DEFAULT NULL,
    PRIMARY KEY (id)
)
