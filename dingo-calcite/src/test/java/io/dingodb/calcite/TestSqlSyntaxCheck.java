/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.calcite;

import io.dingodb.calcite.grammar.ddl.DingoSqlCreateView;
import io.dingodb.calcite.grammar.ddl.SqlAlterAddConstraint;
import io.dingodb.calcite.grammar.ddl.SqlAlterAddForeign;
import io.dingodb.calcite.grammar.ddl.SqlAlterAddIndex;
import io.dingodb.calcite.grammar.ddl.SqlAlterColumn;
import io.dingodb.calcite.grammar.ddl.SqlAlterConstraint;
import io.dingodb.calcite.grammar.ddl.SqlAlterDropConstraint;
import io.dingodb.calcite.grammar.ddl.SqlAlterDropForeign;
import io.dingodb.calcite.grammar.ddl.SqlAlterModifyColumn;
import io.dingodb.calcite.grammar.ddl.SqlAlterTable;
import io.dingodb.calcite.grammar.ddl.SqlCreateIndex;
import io.dingodb.calcite.grammar.ddl.SqlCreateTenant;
import io.dingodb.calcite.grammar.ddl.SqlCreateUser;
import io.dingodb.calcite.grammar.ddl.SqlDropUser;
import io.dingodb.calcite.grammar.ddl.SqlGrant;
import io.dingodb.calcite.grammar.ddl.SqlRevoke;
import io.dingodb.common.log.LogUtils;
import io.dingodb.calcite.grammar.dml.SqlInsert;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.parser.dingo.DingoSqlParserImpl;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.dingodb.calcite.DingoParser.PARSER_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSqlSyntaxCheck {

    @Test
    public void createUser() {
        String sql = "CREATE USER 'gj' IDENTIFIED BY 'abc'";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlCreateUser;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void createTenant() {
        String sql = "CREATE TENANT test_tenant";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlCreateTenant;
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    @Test
    public void dropUser() {
        String sql = "drop USER gj@localhost";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlDropUser;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void grant() {
        String sql = "grant create user on dingo.* to 'gjn'";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlGrant;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void grant2() {
        String sql = "grant grant on *.* to gjn";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlGrant;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void revoke() {
        String sql = "revoke select,update on dingo.userinfo from 'gjn'@'localhost'";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlRevoke;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void addConstraint() {
        //String sql = "alter table t1 add constraint t2 check(a>10)";
        //String sql = "alter table t1 add constraint t2 check(a>10) enforced";
        String sql = "alter table t1 add constraint check(a>10) not enforced";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlAlterAddConstraint;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void addConstraintUnique() {
        //String sql = "alter table t1 add constraint t2 check(a>10)";
        //String sql = "alter table t1 add constraint t2 check(a>10) enforced";
        String sql = "alter table t1 add constraint unique key u2(age)";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlAlterAddIndex;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void addUniqueIndex() {
        String sql = "alter table t1 add unique key u2(age)";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlAlterAddIndex;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void dropConstraint() {
        String sql = "alter table t1 drop constraint t2";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlAlterDropConstraint;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void alterConstraint() {
        String sql = "alter table t1 alter constraint t2 enforced";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlAlterConstraint;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void alterConstraintForeign() {
        //String sql = "alter table t1 add constraint foreign key (id,name) references t2(id,name) on update no action";
        String sql = "alter table t1 add constraint foreign key (id,name) references t2(id,name) "
            + "on update no action on delete CASCADE";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlAlterAddForeign;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void createTableWithForeign() {
        String sql = "create table t1(id int,age int(10),name int, info varchar(20),primary key(id), constraint foreign key (id,name) references t2(id,name) "
            + "on update no action on delete CASCADE)";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlCreateTable;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void sqlAlterDropForeign() {
        String sql = "ALTER TABLE table_name DROP FOREIGN KEY fk_identifier";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlAlterDropForeign;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void sqlAlterModifyColumn() {
        String sql = "ALTER TABLE table_name modify column a int constraint c1 check(a>10) enforced";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlAlterModifyColumn;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void sqlAlterModifyColumn1() {
        String sql = "ALTER TABLE table_name modify column a int references t2(age)";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlAlterModifyColumn;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void sqlAlterMultyModifyColumn1() {
        String sql = "ALTER TABLE table_name modify column a int, modify column name int";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlAlterModifyColumn;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void sqlAlterColumnDropDefault() {
        String sql = "ALTER TABLE table_name alter column a drop default";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlAlterColumn;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void sqlAlterColumnSetDefault() {
        String sql = "ALTER TABLE table_name alter column a set default 'abc'";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlAlterColumn;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void sqlAlterChangeColumn() {
        String sql = "ALTER TABLE `config_info` MODIFY COLUMN `src_ip` varchar(50) CHARACTER SET utf8  DEFAULT NULL COMMENT 'source ip' first";
        SqlParser parser = SqlParser.create(sql, PARSER_CONFIG);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlAlterModifyColumn;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void sqlCreateIndex() {
        String sql = "create fulltext index ix on tx(col1(10) asc, col2(20)) using btree comment 'commitsss' "
            + "algorithm=inplace "
            + "lock =none";
        SqlParser parser = SqlParser.create(sql, PARSER_CONFIG);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlCreateIndex;
            SqlCreateIndex sqlCreateIndex = (SqlCreateIndex) sqlNode;
            assert sqlCreateIndex.mode.contentEquals("fulltext");
            assert sqlCreateIndex.properties.get("comment").equals("commitsss");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void sqlAlterAddFulltextKey() {
        String sql = "alter table t1 add fulltext key ix(age) using btree comment 'commitsss' algorithm=inplace lock=none";
        SqlParser parser = SqlParser.create(sql, PARSER_CONFIG);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlAlterAddIndex;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void sqlCreateView() {
        String sql = "create algorithm=merge definer=dingo sql security invoker view v1 as select * from t1";
        SqlParser parser = SqlParser.create(sql, PARSER_CONFIG);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof DingoSqlCreateView;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void sqlCreateFullTextIndex() {
        String sql = "alter table t1 add fulltext key ix1 (age)";
        SqlParser parser = SqlParser.create(sql, PARSER_CONFIG);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlAlterAddIndex;
            SqlAlterAddIndex sqlAlterAddIndex = (SqlAlterAddIndex) sqlNode;
            assert sqlAlterAddIndex.getIndexDeclaration().mode.equalsIgnoreCase("fulltext");
            assert !sqlAlterAddIndex.getIndexDeclaration().unique;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void sqlAlterAddIndex() {
        String sql = "alter table t1 add index if not exists ix1 (age)";
        SqlParser parser = SqlParser.create(sql, PARSER_CONFIG);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlAlterAddIndex;
            SqlAlterAddIndex sqlAlterAddIndex = (SqlAlterAddIndex) sqlNode;
            assert !sqlAlterAddIndex.getIndexDeclaration().unique;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void sqlIndexTypeOpt() {
        String sql = "CREATE TABLE `roles_txnlsm` (\n" +
            "\t`username` varchar(50) NOT NULL,\n" +
            "\t`role` varchar(50) NOT NULL,\n" +
            "\tUNIQUE INDEX `idx_user_role` (`username` ASC, `role` ASC) USING BTREE\n" +
            ") engine=TXN_LSM";
        SqlParser parser = SqlParser.create(sql, PARSER_CONFIG);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlCreateTable;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void sqlConstraintForeign() {
        String sql = "create table t1(id int,age int,name varchar(20),primary key(id), "
           + "constraint foreign key n1(col1,col2) references tbl_name(col1,col2) on update RESTRICT)";
        SqlParser parser = SqlParser.create(sql, PARSER_CONFIG);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlCreateTable;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void sqlDropConstraintForeign() {
        String sql = "alter table t1 drop foreign key ke";
        SqlParser parser = SqlParser.create(sql, PARSER_CONFIG);
        try {
            SqlNode sqlNode = parser.parseStmt();
            assert sqlNode instanceof SqlAlterTable;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    public void testConstraintCheck() {
        List<String> sqlList = new ArrayList<>();
        sqlList.add("create table t1(id int,age int CONSTRAINT t1 check (age>1) not enforced)");
        sqlList.add("create table t1(id int,age int CONSTRAINT t1 check (age>1) enforced)");
        sqlList.add("create table t1(id int,age int CONSTRAINT t1 check (age>1) not null)");
        sqlList.add("create table t1(id int,age int CONSTRAINT t1 check (age>1) null)");
        sqlList.add("create table t1(id int,age int CONSTRAINT check (age>1))");
        sqlList.add("create table t1(id int,age int, constraint primary key(id))");
        sqlList.add("alter table t1 add constraint c1 check(a>1) not enforced");
        sqlList.add("alter table t1 add constraint check(a>1) not enforced");
        sqlList.add("alter table t1 add constraint check(a>1)");
        sqlList.add("alter table t1 add constraint check(a>1) enforced");
        sqlList.add("alter table t1 drop constraint name1");
        sqlList.add("alter table t1 alter constraint name1 not enforced");
        sqlList.add("alter table t1 alter constraint name1 enforced");
        sqlList.add("create table t1(id int,age int check(age>10) not enforced,primary key(id))");
        sqlList.add("create table t1(id int,age int check(age>10) not null,primary key(id))");
        sqlList.add("create table t1(id int,age int check(age>10) null,primary key(id))");
        sqlList.add("create table t1(id int,age int,primary key(id), constraint check(age>10) not enforced)");
        sqlList.add("alter table t1 add constraint name1 unique key t1 using btree (age)");
        sqlList.add("alter table t1 add constraint unique key t1 (age)");
        sqlList.add("alter table t1 add constraint unique t1 (age)");
        sqlList.add("alter table t1 add unique t1 (age)");
        sqlList.add("create table t1(id int,age int,name varchar(20),primary key(id), "
            + "constraint foreign key (col1,col2) references tbl_name(col1,col2) match partial "
            + "on update CASCADE)");
        sqlList.add("create table t1(id int,age int references t2(age) match full on update cascade)");
        sqlList.add("alter table t1 add constraint c1 foreign key f1 (age,name) references t2(age,name) "
            + "match full on update RESTRICT");
        sqlList.add("ALTER TABLE table_name DROP FOREIGN KEY fk_identifier");
        for (String sql : sqlList) {
            assertTrue(isValidEntry(sql), "syntax check error,sql:" + sql);
        }
    }

    @Test
    public void testAlterTable() {
        List<String> sqlList = new ArrayList<>();
        sqlList.add("ALTER TABLE TBL ALTER COLUMN C1 SET DEFAULT 'A2'");
        sqlList.add("ALTER TABLE TBL ALTER COLUMN C1 DROP DEFAULT");
        sqlList.add("alter table table1 change column column1 column2 decimal(10,1) DEFAULT NULL COMMENT '注释'");
        sqlList.add("alter table table1 change column1 column2 decimal(10,1) DEFAULT NULL COMMENT '注释'");

        // modify column
        sqlList.add("alter table t1 modify column col1 int not null");
        sqlList.add("alter table t1 modify col1 int not null");
        sqlList.add("alter table t1 modify col1 int");
        sqlList.add("alter ignore table t1 modify col1 int");
        sqlList.add("alter ignore table t1 modify col1 int auto_increment");
        sqlList.add("alter ignore table t1 modify col1 int default val");
        sqlList.add("alter ignore table t1 modify col1 int on update current_timestamp comment 'ss'");
        sqlList.add("alter ignore table t1 modify col1 int constraint check(col1>10) not enforced");
        sqlList.add("alter ignore table t1 modify col1 int references tbl(age) match full on update RESTRICT");
        sqlList.add("ALTER TABLE table_name AUTO_INCREMENT=310");
        sqlList.add("rename table t1 to t2");
        sqlList.add("alter table t1 comment='test'");
        sqlList.add("ALTER TABLE tbl_name RENAME INDEX old_index_name TO new_index_name, ALGORITHM=INPLACE, LOCK=NONE");
        for (String sql : sqlList) {
            assertTrue(isValidEntry(sql), "syntax check error,sql:" + sql);
        }
    }

    @Test
    public void testLoadData() {
        List<String> sqlList = new ArrayList<>();
        sqlList.add("load data infile '/xx/data' into table t1 lines terminated by 'x' starting by 'a' "
            + " fields terminated by ','");
        for (String sql : sqlList) {
            assertTrue(isValidEntry(sql), "syntax check error,sql:" + sql);
        }
    }

    @Test
    public void testDtl() {
        List<String> sqlList = new ArrayList<>();
        sqlList.add("CREATE TABLE `gcpbs`.`test3`(`id`  int       NOT NULL   ,\n" +
            "`age`  int       NULL\n" +
            ", PRIMARY KEY (`id`)) engine=InnoDB DEFAULT CHARSET=`utf8mb4` DEFAULT COLLATE `utf8mb4_0900_ai_ci` ROW_FORMAT= Dynamic");
        sqlList.add("SELECT PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME LIKE 'keyring_rds'");
        sqlList.add("LOAD DATA CONCURRENT LOCAL INFILE 'a.csv' ignore INTO TABLE `gcpbs`.`gcp_bs_audit_log` CHARACTER SET utf8mb4 FIELDS TERMINATED BY '\\t' ENCLOSED BY '\"' ESCAPED BY '\\\\' (@`id`,`event_time`,@`act_code`,`is_success`,@`request`,@`response`,@`tenant_id`,@`tenant_name`,@`user_id`,@`user_name`,@`content`,@`module`,@`action`,`tenant_category`) SET `id` = @`id`,`act_code` = UNHEX(@`act_code`),`request` = UNHEX(@`request`),`response` = UNHEX(@`response`),`tenant_id` = UNHEX(@`tenant_id`),`tenant_name` = UNHEX(@`tenant_name`),`user_id` = UNHEX(@`user_id`),`user_name` = UNHEX(@`user_name`),`content` = UNHEX(@`content`),`module` = UNHEX(@`module`),`action` = UNHEX(@`action`)");
        for (String sql : sqlList) {
            assertTrue(isValidEntry(sql), "syntax check error,sql:" + sql);
        }
    }

    public void testSqlParseXml() {
        String sql = "INSERT INTO `gcp_report_template` (`id`, `name`, `content`, `created_time`, `last_update_time`, `is_deleted`) VALUES (1, 'userStockByDayStats.ureport.xml', '<?xml version=\\\"1.0\\\" encoding=\\\"UTF-8\\\"?><ureport><cell expand=\\\"None\\\" name=\\\"A1\\\" row=\\\"1\\\" col=\\\"1\\\"><cell-style font-size=\\\"10\\\" font-family=\\\"宋体\\\" bgcolor=\\\"170,161,161\\\" bold=\\\"true\\\" align=\\\"center\\\" valign=\\\"middle\\\"><left-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/><right-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/><top-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/><bottom-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/></cell-style><simple-value><![CDATA[date]]></simple-value></cell><cell expand=\\\"None\\\" name=\\\"B1\\\" row=\\\"1\\\" col=\\\"2\\\"><cell-style font-size=\\\"10\\\" font-family=\\\"宋体\\\" bgcolor=\\\"170,161,161\\\" bold=\\\"true\\\" align=\\\"center\\\" valign=\\\"middle\\\"><left-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/><right-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/><top-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/><bottom-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/></cell-style><simple-value><![CDATA[number]]></simple-value></cell><cell expand=\\\"None\\\" name=\\\"C1\\\" row=\\\"1\\\" col=\\\"3\\\"><cell-style font-size=\\\"10\\\" font-family=\\\"宋体\\\" bgcolor=\\\"170,161,161\\\" bold=\\\"true\\\" align=\\\"center\\\" valign=\\\"middle\\\"><left-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/><right-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/><top-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/><bottom-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/></cell-style><simple-value><![CDATA[count]]></simple-value></cell><cell expand=\\\"Down\\\" name=\\\"A2\\\" row=\\\"2\\\" col=\\\"1\\\"><cell-style font-size=\\\"10\\\" font-family=\\\"宋体\\\" align=\\\"center\\\" valign=\\\"middle\\\"><left-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/><right-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/><top-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/><bottom-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/></cell-style><dataset-value dataset-name=\\\"userPerDayStockStats\\\" aggregate=\\\"group\\\" property=\\\"dt\\\" order=\\\"asc\\\" mapping-type=\\\"simple\\\"></dataset-value></cell><cell expand=\\\"None\\\" name=\\\"B2\\\" row=\\\"2\\\" col=\\\"2\\\"><cell-style font-size=\\\"10\\\" font-family=\\\"宋体\\\" align=\\\"center\\\" valign=\\\"middle\\\"><left-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/><right-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/><top-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/><bottom-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/></cell-style><dataset-value dataset-name=\\\"userPerDayStockStats\\\" aggregate=\\\"sum\\\" property=\\\"num\\\" order=\\\"none\\\" mapping-type=\\\"simple\\\"></dataset-value></cell><cell expand=\\\"None\\\" name=\\\"C2\\\" row=\\\"2\\\" col=\\\"3\\\"><cell-style font-size=\\\"10\\\" font-family=\\\"宋体\\\" align=\\\"center\\\" valign=\\\"middle\\\"><left-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/><right-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/><top-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/><bottom-border width=\\\"1\\\" style=\\\"solid\\\" color=\\\"0,0,0\\\"/></cell-style><expression-value><![CDATA[if(&A2==1){\\r\\n return B2;\\r\\n}else{\\r\\n   B2 + C2[A2:-1]\\r\\n}]]></expression-value></cell><row row-number=\\\"1\\\" height=\\\"18\\\"/><row row-number=\\\"2\\\" height=\\\"18\\\"/><column col-number=\\\"1\\\" width=\\\"80\\\"/><column col-number=\\\"2\\\" width=\\\"80\\\"/><column col-number=\\\"3\\\" width=\\\"83\\\"/><datasource name=\\\"InnerDataSource\\\" type=\\\"buildin\\\"><dataset name=\\\"userPerDayStockStats\\\" type=\\\"sql\\\"><sql><![CDATA[WITH RECURSIVE date_sequence AS (\\r\\n  SELECT\\r\\n    DATE_ADD(curdate() , INTERVAL -:days DAY) AS date\\r\\n  UNION ALL\\r\\n  SELECT\\r\\n    DATE_ADD(date, INTERVAL 1 DAY)\\r\\n  FROM\\r\\n    date_sequence\\r\\n  WHERE\\r\\n    date < DATE_ADD(curdate() , INTERVAL -1 DAY)\\r\\n)\\r\\nselect DATE_ADD(curdate() , INTERVAL -:days DAY) dt,count(*) num from gcp_bs_tenant t where t.is_deleted =0 and date_format(t.created_time,\\'%Y-%m-%d\\') <=DATE_ADD(curdate() , INTERVAL -:days DAY)\\r\\nunion all\\r\\nSELECT date dt,0 FROM date_sequence\\r\\nunion all\\r\\nselect date_format(t.created_time,\\'%Y-%m-%d\\'),count(*) num from gcp_bs_tenant t where t.is_deleted =0 and date_format(t.created_time,\\'%Y-%m-%d\\') >DATE_ADD(curdate() , INTERVAL -:days DAY) and date_format(t.created_time,\\'%Y-%m-%d\\') <=DATE_ADD(curdate() , INTERVAL -1 DAY) group by date_format(t.created_time,\\'%Y-%m-%d\\')]]></sql><field name=\\\"dt\\\"/><field name=\\\"num\\\"/><parameter name=\\\"days\\\" type=\\\"Integer\\\" default-value=\\\"14\\\"/></dataset></datasource><paper type=\\\"A4\\\" left-margin=\\\"90\\\" right-margin=\\\"90\\\"\\r\\n    top-margin=\\\"72\\\" bottom-margin=\\\"72\\\" paging-mode=\\\"fitpage\\\" fixrows=\\\"0\\\"\\r\\n    width=\\\"595\\\" height=\\\"842\\\" orientation=\\\"portrait\\\" html-report-align=\\\"left\\\" bg-image=\\\"\\\" html-interval-refresh-value=\\\"0\\\" column-enabled=\\\"false\\\"></paper></ureport>', '2024-08-09 16:03:19', '2024-08-09 16:04:17', 0) ";
        SqlParser parser = SqlParser.create(sql, PARSER_CONFIG);
        try {
            SqlNode sqlNode = parser.parseStmt();
            SqlInsert sqlInsert = (SqlInsert) sqlNode;
            SqlBasicCall call = (SqlBasicCall) sqlInsert.getSource();
            SqlBasicCall subCall = call.operand(0);
            assertEquals(6, subCall.getOperandList().size());
        } catch (Exception e) {
            assertTrue(false, "syntax check error,sql:" + sql);
        }
    }

    @Test
    public void testSqlChineseCharacter() {
        List<String> sqlList = new ArrayList<>();
        sqlList.add("select name as 使用时长（小时）, age as sf from test1");
        sqlList.add("create table t1(id int,age varchar(20) comment 'as;sdfas\\'a\"sdf')");
        sqlList.add("create table t1(id int,`value` varchar(64) CHARACTER SET `utf8mb3` COLLATE utf8mb3_general_ci DEFAULT NULL)");
        sqlList.add("CREATE TABLE `gcp_bs_charge_detail_head` (\n" +
            "`id` bigint NOT NULL AUTO_INCREMENT,\n" +
            "`charge_code` varchar(100) DEFAULT NULL COMMENT '详单编号（交易单号）,所有计费详单统一编号（跨表），全局唯一,Txn+交易日期-账户ID-随机数\\n例如：Txn-20240110-12345678-随机数',\n" +
            "`event_type` int DEFAULT NULL COMMENT '事件类型：\\r\\n1-Slurm用量事件（Slurm计费详单）\\r\\n2-预付订单结清事件（包年包月计费详单）\\r\\n3-预付订单退费事件（包年包月计费详单）\\r\\n4-后付订单日结事件（包年包月计费详单）',\n" +
            "`tenant_id` varchar(100) DEFAULT NULL,\n" +
            "`user_name` varchar(100) DEFAULT NULL COMMENT '用户名,对应租户的主账号,即gcp_bs_user.username且 is_manager=true',\n" +
            "`account_id` bigint DEFAULT NULL COMMENT '账户编号',\n" +
            "`resource_type_id` bigint DEFAULT NULL COMMENT '资源类型id',\n" +
            "`product_id` bigint DEFAULT NULL COMMENT '产品id',\n" +
            "`product_code` varchar(100) DEFAULT NULL COMMENT '产品编号',\n" +
            "`charge_method` int DEFAULT NULL COMMENT '1-包年包月 2-按量计费',\n" +
            "`charge_type` int DEFAULT NULL COMMENT '1- OC - 预付费（一次付清）2- \\nRC - 后付费（月付账单）\\n3- UC - 实时付费（实时付费）',\n" +
            "`start_time` datetime DEFAULT NULL COMMENT '计费周期开始时间,对于预付费，对应订单开始时间;对于后付费，对应日结计费周期的开始时间;对于实时付费，对应详单的小时计费周期开始时间',\n" +
            "`end_time` datetime DEFAULT NULL COMMENT '计费周期的结束时间,对于预付费，对应订单终止时间;对于后付费，对应日结计费周期的结束时间;对于实时付费，对应详单的小时计费周期结束时间',\n" +
            "`order_id` bigint DEFAULT NULL COMMENT '订单表的主键id',\n" +
            "`order_code` varchar(100) DEFAULT NULL COMMENT '订单编号',\n" +
            "`instance_id` varchar(100) DEFAULT NULL COMMENT '产品实例编号',\n" +
            "`aidc_id` bigint DEFAULT NULL COMMENT '智算中心id',\n" +
            "`charge_item` int DEFAULT NULL,\n" +
            "`charge_start_time` datetime DEFAULT NULL COMMENT '计费开始时间，对于预付费，对应订单开始时间;对于后付费，对应日计费周期的开始时间(下单当日为订单开始时间);对于实时付费，对应Max(任务开始时间，本次计费周期开始时间)',\n" +
            "`charge_end_time` datetime DEFAULT NULL COMMENT '计费结束时间,对于预付费，对应订单终止时间;对于后付费，对应日计费结束时间(订单到期日为订单终止时间);对于实时付费，对应Min(任务结束时间，本次计费周期结束时间)',\n" +
            "`charge_end_reason` varchar(100) DEFAULT NULL COMMENT '作业正常结束;作业掉卡;作业资源被抢占;实例到期;实例变更（升级/降级）;实例终止退费',\n" +
            "`charge_seconds` bigint DEFAULT NULL COMMENT '计量时长（秒）= 计量结束时间 - 计量开始时间',\n" +
            "`charge_value` decimal(24,8) DEFAULT NULL COMMENT '计量值,通过产品计费项的计量公式计算得到,定点数四舍五入保留八位小数',\n" +
            "`charge_unit` int DEFAULT NULL COMMENT '计量单位,与产品计费项的计量单位对齐',\n" +
            "`unit_price` decimal(16,4) DEFAULT NULL COMMENT '通过产品计费项的单价公式计算得到单价,定点数四舍五入保留四位小数，单位：元',\n" +
            "`charge_amt` decimal(16,4) DEFAULT NULL COMMENT '计费金额=计费单价×计量值，定点数四舍五入保留四位小数，单位：元',\n" +
            "`discount_type` int DEFAULT NULL COMMENT '1-折扣,2-\\n直减',\n" +
            "`discount_value` decimal(16,4) DEFAULT NULL COMMENT '优惠方式为折扣时表示折扣率，优惠方式为直减时代表单价下调金额（元）',\n" +
            "`discount_amt` decimal(16,4) DEFAULT NULL COMMENT '折扣 优惠金额 = (1-折扣率)×计费单价×计量值；直减 优惠金额 = (计费单价-下调金额)×计量值',\n" +
            "`charge_amt_after_discount` decimal(16,4) DEFAULT NULL COMMENT '优惠后金额=计费金额-优惠金额',\n" +
            "`adjust_amt` decimal(16,4) DEFAULT NULL COMMENT '调整金额',\n" +
            "`adjust_desc` varchar(255) DEFAULT NULL COMMENT '调整描述',\n" +
            "`charge_amt_after_adjust` decimal(16,4) DEFAULT NULL COMMENT '应付金额 = 计费金额 - 优惠金额 - 调整金额',\n" +
            "`bill_cycle` varchar(100) DEFAULT NULL COMMENT 'YYYY-MM',\n" +
            "`bill_item_id` varchar(100) DEFAULT NULL COMMENT '账目编号,出账日生成账单账目',\n" +
            "`detail_status` int DEFAULT NULL COMMENT '账单明细状态   1未支付  2已支付',\n" +
            "`created_time` datetime DEFAULT NULL COMMENT '创建时间',\n" +
            "`last_update_time` datetime DEFAULT NULL COMMENT '最后更新时间',\n" +
            "`user_id` varchar(100) DEFAULT NULL,\n" +
            "`charge_combo` int DEFAULT NULL COMMENT '(12-按量付费周期费用,13-按量付费按量付费,21-包年包月一次结清,22-包年包月周期费用)',\n" +
            "`product_category` int DEFAULT NULL,\n" +
            "`usage_id` varchar(100) DEFAULT NULL COMMENT '计量事件id；',\n" +
            "`is_dcu` tinyint(1) DEFAULT NULL COMMENT '是否dcu账单明细；',\n" +
            "`gpu_type` varchar(100) DEFAULT NULL COMMENT 'GPU类型',\n" +
            "`task_id` varchar(100) DEFAULT NULL COMMENT '任务id',\n" +
            "`task_name` varchar(100) DEFAULT NULL COMMENT '任务名称',\n" +
            "`stats_type` int DEFAULT NULL COMMENT '统计类型：1-独占,2-共享',\n" +
            "`promotion_strategy` varchar(255) DEFAULT NULL COMMENT '优惠策略描述国际化JSON',\n" +
            "`charge_value_before_discount` decimal(24,8) DEFAULT NULL COMMENT '优惠扣减前DCU使用量',\n" +
            "PRIMARY KEY (`id`),\n" +
            "KEY `idx_start_time` (`start_time`),\n" +
            "KEY `idx_order_id` (`order_id`),\n" +
            "KEY `idx_account_id` (`account_id`),\n" +
            "KEY `idx_tenant_id` (`tenant_id`),\n" +
            "KEY `idx_product_id` (`product_id`),\n" +
            "KEY `idx_charge_code` (`charge_code`),\n" +
            "KEY `idx_aidc_id` (`aidc_id`)\n" +
            ") ENGINE=InnoDB AUTO_INCREMENT=944162 DEFAULT CHARSET=utf8mb3");
        for (String sql : sqlList) {
            assertTrue(isValidEntry(sql), "syntax check error,sql:" + sql);
        }
    }

    @Test
    public void split() {
        String sql = "/* DTS-writer-h8ci338813nu7cl-1 */insert into `gcpbs`.`gcp_bs_charge_detail_head` (`charge_end_reason`,`charge_type`,`charge_value`,`gpu_type`,`stats_type`,`charge_method`,`discount_amt`,`event_type`,`resource_type_id`,`product_id`,`id`,`adjust_desc`,`created_time`,`detail_status`,`aidc_id`,`charge_end_time`,`order_code`,`start_time`,`last_update_time`,`instance_id`,`charge_start_time`,`user_id`,`charge_amt`,`charge_amt_after_adjust`,`bill_cycle`,`charge_amt_after_discount`,`promotion_strategy`,`order_id`,`tenant_id`,`task_name`,`charge_seconds`,`user_name`,`charge_code`,`charge_unit`,`adjust_amt`,`task_id`,`product_code`,`usage_id`,`bill_item_id`,`charge_combo`,`charge_value_before_discount`,`charge_item`,`end_time`,`discount_value`,`unit_price`,`discount_type`,`is_dcu`,`account_id`,`product_category`)  VALUES  (NULL, 3, '0E-8', NULL, 2, 2, NULL, 1, 1, 39, 1089961, NULL, '2025-03-13 15:10:54.0', NULL, 1, '2025-03-13 15:00:00.0', x'4F52443230323530323238313633363132363532313335', '2025-03-13 14:00:00.0', '2025-03-13 15:10:54.0', x'65626366363564342D316536342D346537342D396433322D393639626365366263396164', '2025-03-13 14:00:00.0', x'', NULL, NULL, x'323032352D3033', '0.0000', x'7B2253544F524147455F474946545F4341504143495459223A7B22656E2D5553223A224C696D697465642D74696D65206672656520313032344742206F66666572222C227A682D434E223A22E99990E697B6E5858DE8B4B9313032344742E6B4BBE58AA8227D7D', 4924, x'63366664643439372D613564612D343264642D383634382D353932316163626431613736', x'', 3600, x'', x'54584E3230323530333133313531303533353537323632', 8, NULL, x'', x'5052442D53544F524147452D4341504143495459', NULL, NULL, 11, '0.02000000', 1, '2025-03-13 15:00:00.0', NULL, '0.0000', NULL, 1, 1975, 2) ;/* DTS-writer-h8ci338813nu7cl-1 */insert into `gcpbs`.`gcp_bs_measure_event` (`task_name`,`cluster`,`memory`,`gpu_type`,`user_name`,`gpu_total`,`task_id`,`cpu_total`,`source`,`scene`,`partition_id`,`event_type`,`qos`,`id`,`seq`,`timestamp`,`app`,`created_time`,`product_time`,`cpu`,`gpu_mem_total`,`gpu`,`mem_total`,`start_time`,`event_id`,`instance_id`,`user_id`,`job_id`,`end_state`,`instance_type`,`account`,`status`)  VALUES  (x'', NULL, NULL, NULL, x'', NULL, x'', NULL, NULL, x'42534D2D73746F726167652D34333230', NULL, x'53594E435F4556454E545F545950455F4B454550414C495645', NULL, 12360074, 250313906, x'31373431383439353630', NULL, '2025-03-13 15:10:54.0', '2025-03-13 15:06:00.0', NULL, NULL, NULL, NULL, x'31373339343937353335', x'30376431303165372D323962332D343266382D393131352D366632666538656561623937', x'38623435613565352D633335632D343235612D393337352D353434326664323836353233', x'', x'42534D2D73746F726167652D34333230', NULL, x'73746F72616765', NULL, 0) ;/* DTS-writer-h8ci338813nu7cl-1 */insert into `gcpbs`.`gcp_bs_charge_detail_head` (`charge_end_reason`,`charge_type`,`charge_value`,`gpu_type`,`stats_type`,`charge_method`,`discount_amt`,`event_type`,`resource_type_id`,`product_id`,`id`,`adjust_desc`,`created_time`,`detail_status`,`aidc_id`,`charge_end_time`,`order_code`,`start_time`,`last_update_time`,`instance_id`,`charge_start_time`,`user_id`,`charge_amt`,`charge_amt_after_adjust`,`bill_cycle`,`charge_amt_after_discount`,`promotion_strategy`,`order_id`,`tenant_id`,`task_name`,`charge_seconds`,`user_name`,`charge_code`,`charge_unit`,`adjust_amt`,`task_id`,`product_code`,`usage_id`,`bill_item_id`,`charge_combo`,`charge_value_before_discount`,`charge_item`,`end_time`,`discount_value`,`unit_price`,`discount_type`,`is_dcu`,`account_id`,`product_category`)  VALUES  (NULL, 3, '0E-8', NULL, 2, 2, NULL, 1, 1, 39, 1089962, NULL, '2025-03-13 15:10:55.0', NULL, 1, '2025-03-13 15:00:00.0', x'4F52443230323530323134303934353130343438373139', '2025-03-13 14:00:00.0', '2025-03-13 15:10:55.0', x'38623435613565352D633335632D343235612D393337352D353434326664323836353233', '2025-03-13 14:00:00.0', x'', NULL, NULL, x'323032352D3033', '0.0000', x'7B2253544F524147455F474946545F4341504143495459223A7B22656E2D5553223A224C696D697465642D74696D65206672656520313032344742206F66666572222C227A682D434E223A22E99990E697B6E5858DE8B4B9313032344742E6B4BBE58AA8227D7D', 4320, x'63373065393233352D363364312D346663652D613734342D653564613933383864663362', x'', 3600, x'', x'54584E3230323530333133313531303535323637353130', 8, NULL, x'', x'5052442D53544F524147452D4341504143495459', NULL, NULL, 11, '0.02000000', 1, '2025-03-13 15:00:00.0', NULL, '0.0000', NULL, 1, 1217, 2) ";

        if (sql.contains(";/* DTS-writer")) {
            String split = ";/*";
            String[] sqls = sql.split(split);
            for (String splitSql : sqls) {
                try {
                    if (splitSql.startsWith("* DTS-writer")) {
                        splitSql = "/" + splitSql;
                    }
                    System.out.println("--------->" + splitSql);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

    private boolean isValidEntry(String sql) {
        SqlParser parser = SqlParser.create(sql, PARSER_CONFIG);
        try {
            SqlNode sqlNode = parser.parseStmt();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
