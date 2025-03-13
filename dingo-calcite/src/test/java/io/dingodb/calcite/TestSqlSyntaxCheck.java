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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.dingo.DingoSqlParserImpl;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.dingodb.calcite.DingoParser.PARSER_CONFIG;
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
            parser.parseStmt();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}
