CREATE TABLE DINGO.{table} (
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
) AUTO_INCREMENT = 1000 COMMENT='资源信息表'
