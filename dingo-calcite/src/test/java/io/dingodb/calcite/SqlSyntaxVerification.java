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
import io.dingodb.calcite.grammar.ddl.SqlSetPassword;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.dingo.DingoSqlParserImpl;
import org.junit.jupiter.api.Test;

import static io.dingodb.calcite.DingoParser.PARSER_CONFIG;

public class SqlSyntaxVerification {

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

}
