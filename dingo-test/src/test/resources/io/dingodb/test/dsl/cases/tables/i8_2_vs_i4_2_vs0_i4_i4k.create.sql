CREATE TABLE DINGO.{table} (
    create_time BIGINT NOT NULL,
    update_time BIGINT NOT NULL,
    type VARCHAR(255) NOT NULL,
    operation_id INTEGER NOT NULL,
    resource_id INTEGER NOT NULL,
    remark VARCHAR(255),
    status INTEGER NOT NULL,
    id INTEGER NOT NULL AUTO_INCREMENT,
    PRIMARY KEY (id)
) AUTO_INCREMENT = 1000 COMMENT='权限信息表'
