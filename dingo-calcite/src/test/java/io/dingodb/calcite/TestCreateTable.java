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

import io.dingodb.calcite.grammar.ddl.SqlGrant;
import io.dingodb.calcite.grammar.ddl.SqlSetPassword;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlKeyConstraint;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.dingo.DingoSqlParserImpl;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TestCreateTable {

    public static void parseCreateTable(SqlCreateTable create) {
        List<String> keyList = null;
        for (SqlNode sqlNode : create.columnList) {
            if (sqlNode instanceof SqlKeyConstraint) {
                SqlKeyConstraint constraint = (SqlKeyConstraint) sqlNode;
                if (constraint.getOperator().getKind() == SqlKind.PRIMARY_KEY) {
                    // The 0th element is the name of the constraint
                    keyList = ((SqlNodeList) constraint.getOperandList().get(1)).getList().stream()
                        .map(t -> ((SqlIdentifier) Objects.requireNonNull(t)).getSimple())
                        .collect(Collectors.toList());
                    break;
                }
            }
        }

        for (String key : keyList) {
            System.out.println("---> primary key:" + key);
        }
    }

    public static void main(String[] args) {
        System.out.println("---");
    }

    @Test
    public void createUser() {
        String sql = "CREATE USER 'gj' IDENTIFIED BY 'abc'";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            System.out.println("---> sqlNode:" + sqlNode);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Test
    public void createUserWithLocation() {
        String sql = "CREATE USER 'gj'@localhost IDENTIFIED BY 'abc'";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            System.out.println("---> sqlNode:" + sqlNode);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Test
    public void dropUser() {
        String sql = "drop USER gj@localhost";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            System.out.println("---> sqlNode:" + sqlNode);
        } catch (Exception e) {
            e.printStackTrace();
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
            System.out.println("---> sqlNode:" + sqlNode);
        } catch (Exception e) {
            e.printStackTrace();
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
            SqlGrant sqlGrant = (SqlGrant) sqlNode;
            System.out.println(sqlGrant.privileges.size());
            System.out.println("---> sqlNode:" + sqlNode);
        } catch (Exception e) {
            e.printStackTrace();
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
            System.out.println("---> sqlNode:" + sqlNode);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Test
    public void flush() {
        String sql = "flush privileges";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            System.out.println("---> sqlNode:" + sqlNode);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Test
    public void setPassword() {
        String sql = "set password for gjn = password('123')";
        SqlParser.Config config = SqlParser.config().withParserFactory(DingoSqlParserImpl::new);
        SqlParser parser = SqlParser.create(sql, config);
        try {
            SqlNode sqlNode = parser.parseStmt();
            SqlSetPassword sqlSetPassword = (SqlSetPassword) sqlNode;
            System.out.println(sqlSetPassword.user + ", host:" + sqlSetPassword.host
                + ",pwd" + sqlSetPassword.password);
            System.out.println("---> sqlNode:" + sqlNode);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}
