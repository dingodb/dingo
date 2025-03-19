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
        for (String sql : sqlList) {
            assertTrue(isValidEntry(sql), "syntax check error,sql:" + sql);
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
