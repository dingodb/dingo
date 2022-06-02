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

package io.dingodb.test.exception;

import io.dingodb.exec.Services;
import io.dingodb.meta.test.MetaTestService;
import io.dingodb.test.SqlHelper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class ExceptionTest {
    private static Connection connection;
    private static SqlHelper sqlHelper;

    @BeforeAll
    public static void setupAll() throws Exception {
        connection = (sqlHelper = new SqlHelper()).getConnection();
    }


    @AfterAll
    public static void cleanUpAll() throws Exception {
        connection.close();
        Services.metaServices.get(MetaTestService.SCHEMA_NAME).clear();
    }

    // Table not found (90002)
    @Test
    public void testInsertDateException1() {
        String createTableSql = "create table datetest1(id int, name varchar(20), age int, "
            + "amount double, create_date date, primary key (id))";
        String insertSql = "insert into datetest1 values(1, 'zhang san', 18, 1342.09, '1988-11-11')";
        String selectSql = "SELECT * from datetest4";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage()).isEqualTo("Error 90002 (00000) : Error while executing SQL "
                + "\"insert into datetest4 values(1, 'zhang san', 18, 1342.09, '88-11-11')\": From line 1, "
                + "column 13 to line 1, column 21: Object 'DATETEST4' not found");
        }
    }

    // Insert Time Format Error (90019)
    @Test
    public void testInsertDateException2() {
        String createTableSql = "create table datetest2(id int, name varchar(20), age int, "
            + "amount double, create_date date, primary key (id))";
        String insertSql = "insert into datetest2 values(1, 'zhang san', 18, 1342.09, '88-11-11')";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage()).isEqualTo("Error 90019 (00000) : Error while executing SQL "
                + "\"insert into datetest2 values(1, 'zhang san', 18, 1342.09, '88-11-11')\": Error in parsing"
                + " \"88-11-11 does not match any of the datetime pattern yyyyMMdd, yyyy-MM-dd , yyyy.MM.dd, "
                + "yyyy/MM/dd\" to time/date/datetime");
        }
    }

    // Table not found (90002)
    @Test
    public void testInsertDateException3() {
        String createTableSql = "create table datetest2_1(id int, name varchar(20), age int, "
            + "amount double, create_date date, primary key (id))";
        String insertSql = "insert into datetest2_1 select * from datetest2_2";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage()).isEqualTo("Error 90002 (00000) : Error while executing SQL \"insert "
                + "into datetest2_1 select * from datetest2_2\": From line 1, column 39 to line 1, column 49: "
                + "Object 'DATETEST2_2' not found");
        }
    }

    // Table not found (90002)
    @Test
    public void testSelectDateException1() {
        String createTableSql = "create table datetest3(id int, name varchar(20), age int, "
            + "amount double, create_date date, primary key (id))";
        String insertSql = "insert into datetest3 values(1, 'zhang san', 18, 1342.09, '1988-11-11')";
        String selectSql = "SELECT * from datetest5";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            statement.executeQuery(selectSql);
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage()).isEqualTo("Error 90002 (00000) : Error while executing SQL \"SELECT * from "
                + "datetest5\": From line 1, column 15 to line 1, column 23: Object 'DATETEST5' not found");
        }
    }

    // Table not found (90002)
    @Test
    public void testDeleteException() {
        String createTableSql = "create table datetest4(id int, name varchar(20), age int, "
            + "amount double, create_date date, primary key (id))";
        String insertSql = "insert into datetest4 values(1, 'zhang san', 18, 1342.09, '1988-11-11')";
        String deleteSql = "DELETE from datetest5";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            statement.executeQuery(deleteSql);
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage()).isEqualTo("Error 90002 (00000) : Error while executing SQL \"DELETE "
                + "from datetest5\": From line 1, column 13 to line 1, column 21: Object 'DATETEST5' not found");
        }
    }

    // Table not found (90002)
    @Test
    public void testTimeFormatException() throws SQLException {
        String sql = "select time_format(240000, '%H.%i.%s')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
            } catch (Exception e) {
                System.out.println("Result: ");
                System.out.println(e.getMessage());
                assertThat(e.getMessage()).isEqualTo("Error 90019 (00000) : Error while executing SQL "
                    + "\"select time_format(240000, '%H.%i.%s')\": Error in parsing \"240000 can be less than 240000\""
                    + " to time/date/datetime");
            }
        }
    }

    // Table not found (90002)
    @Test
    public void testTimeFormatException1() throws SQLException {
        String sql = "select time_format('240001', '%H.%i.%s')";
        try (Statement statement = connection.createStatement()) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.println("Result: ");
                System.out.println(rs.getString(0));
            } catch (Exception e) {
                System.out.println("Result: ");
                System.out.println(e.getMessage());
                assertThat(e.getMessage()).isEqualTo("Error 90019 (00000) : Error while executing SQL "
                    + "\"select time_format('240001', '%H.%i.%s')\": Error in parsing "
                    + "\"240001 can be less than 240000\""
                    + " to time/date/datetime");
            }
        }
    }

    // Table Already exists (90007)
    @Test
    public void testTableAlreadyExist() throws SQLException {
        String createTableSql = "create table test1(id int, name varchar(20), age int, amount double,"
            + " address varchar(255),update_time time,primary key (id))";
        String createTableSql1 = "create table test1(id int, age int, amount double, primary key (id))";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.execute(createTableSql1);
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage()).isEqualTo("Error 90007 (00000) : Error while executing SQL \"create table "
                + "test1(id int, age int, amount double, primary key (id))\": From line 1, column 14 to line 1, "
                + "column 18: Table 'TEST1' already exists");
        }
    }

    // Insert Column Number not match (90013).
    @Test
    public void testInsertColumnNumberNotMatch() throws SQLException {
        String createTableSql = "create table test2(id int, name varchar(20), age int, amount double,"
            + " address varchar(255),update_time time,primary key (id))";
        String insertSql = "insert into test2 values('xiaoY')";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage()).isEqualTo("Error 90013 (00000) : Error while executing SQL "
                + "\"insert into test2 "
                + "values('xiaoY')\": From line 1, column 13 to line 1, column 17: Number of "
                + "INSERT target columns (6) "
                + "does not equal number of source items (1)");
        }
    }

    // Column not found when insert (90002)
    @Test
    public void testColumnNotExist() throws SQLException {
        String createTableSql = "create table test3(id int, name varchar(20), age int, amount double, address "
            + "varchar(255), birthday date, create_time time ,update_time timestamp, primary key (id))";
        String insertSql = "insert into test3 (id, name1, age, amount, address, birthday, create_time, update_time,"
            + " is_delete) values (1,'zhangsan',18,23.50,'beijing','1998-4-6', '08:10:10', "
            + "'2022-4-8 18:05:07')";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.execute(insertSql);
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage()).isEqualTo("Error 90002 (00000) : Error while executing SQL \"insert into "
                + "test3 (id, name1, age, amount, address, birthday, create_time, update_time, is_delete) values"
                + " (1,'zhangsan',18,23.50,'beijing','1998-4-6', '08:10:10', '2022-4-8 18:05:07')\": "
                + "From line 1, column 24 to line 1, column 28: Unknown target column 'NAME1'");
        }
    }

    // Column not found when select (90002)
    @Test
    public void testSelectColumnNotExist() throws SQLException {
        String createTableSql = "create table test3_1(id int, name varchar(20), age int, amount double,address "
            + "varchar(255), birthday date, create_time time ,update_time timestamp, primary key (id))";
        String insertSql = "insert into test3_1 (id, name, age, amount, address, birthday, create_time, update_time"
            + ") values (1,'zhangsan',18,23.50,'beijing','1998-4-6', '08:10:10', "
            + "'2022-4-8 18:05:07')";
        String selectSql = "SELECT name1 from test3_1";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.execute(insertSql);
            statement.executeQuery(selectSql);
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage()).isEqualTo("Error 90002 (00000) : Error while executing SQL \"SELECT name1 "
                + "from test3_1\": From line 1, column 8 to line 1, column 12: Column 'NAME1' not found in any table");
        }
    }

    // Column not found when select (90002)
    @Test
    public void testUpdateColumnNotExist() throws SQLException {
        String createTableSql = "create table test3_2(id int, name varchar(20), age int, amount double,address "
            + "varchar(255), birthday date, create_time time ,update_time timestamp, primary key (id))";
        String insertSql = "insert into test3_2 (id, name, age, amount, address, birthday, create_time, update_time"
            + ") values (1,'zhangsan',18,23.50,'beijing','1998-4-6', '08:10:10', "
            + "'2022-4-8 18:05:07')";
        String updateSql = "update test3_1 set name1 = 'xiaoH'";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.execute(insertSql);
            statement.executeQuery(updateSql);
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage()).isEqualTo("Error 90002 (00000) : Error while executing SQL \"update test3_1 "
                + "set name1 = 'xiaoH'\": From line 1, column 8 to line 1, column 14: Object 'TEST3_1' not found");
        }
    }

    // Insert column more than once (90011)
    @Test
    public void testColumnInertMoreThanOnce() throws SQLException {
        String createTableSql = "create table test4(id int, name varchar(20), age int, amount double,"
            + " address varchar(255),update_time time,primary key (id))";
        String insertSql = "insert into test4 (id, id, name, age, amount, address, birthday, create_time, update_time, "
            + "is_delete) values (1,1,'zhangsan',18,23.50,'beijing','1998-4-6', '08:10:10', '2022-4-8 18:05:07', true)";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.execute(insertSql);
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage()).isEqualTo("Error 90011 (00000) : Error while executing SQL \"insert into test4 "
                + "(id, id, name, age, amount, address, birthday, create_time, update_time, is_delete) values (1,1,"
                + "'zhangsan',18,23.50,'beijing','1998-4-6', '08:10:10', '2022-4-8 18:05:07', true)\": From line 1, "
                + "column 24 to line 1, column 25: Target column 'ID' is assigned more than once");
        }
    }

    // Duplicated Columns Error (90009)
    @Test
    public void testDuplicatedColumn() throws SQLException {
        String createTableSql = "create table test5(id int, id int, name varchar(20), age int, amount double,"
            + " address varchar(255), birthday date, create_time time, update_time timestamp,"
            + " is_delete boolean, primary key (id))";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage()).isEqualTo("Error 90009 (00000) : Error while executing SQL \"create table"
                + " test5(id int, id int, name varchar(20), age int, amount double, "
                + "address varchar(255), birthday date, create_time time, update_time timestamp, "
                + "is_delete boolean, primary key (id))\": Duplicate column names are not allowed in "
                + "table definition. Total: 10, distinct: 9");
        }
    }

    // Primary key required (90010)
    @Test
    public void testPrimaryColumn() throws SQLException {
        String createTableSql = "create table test6(id int, id int, name varchar(20), age int, amount double,"
            + " address varchar(255), birthday date, create_time time, update_time timestamp,"
            + " is_delete boolean)";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage()).isEqualTo("Error 90010 (00000) : Error while executing SQL \"create "
                + "table test6(id int, id int, name varchar(20), age int, amount double, address varchar(255), "
                + "birthday date, create_time time, update_time timestamp, is_delete boolean)\": "
                + "Primary keys are required in table definition.");
        }
    }

    @Test
    public void testInsertTime4() throws SQLException {
        String createTableSql = "create table timetest4(id int, update_time time,primary key (id))";
        String insertSql = "insert into timetest4 values(1,'')";
        String selectSql = "select * from timetest4";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            ResultSet rs = statement.executeQuery(selectSql);
            System.out.println("Result: ");
            while (rs.next()) {
                System.out.println(rs.getString(1));
                System.out.println(rs.getString(2));
            }
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage().replaceAll("(?<=#)\\d+", "")).isEqualTo("Error 90019 "
                + "(00000) : Error while executing SQL"
                + " \"insert into timetest4 values(1,'')\": Error in parsing \" '' is not allowed to convert "
                + "to time type.\" to time/date/datetime");
        }
    }

    // Insert type not match
    @Test
    public void testColumnTypeNotMatch() throws SQLException {
        String createTableSql = "create table test7_1(id int, name varchar(256), age int, primary key(id))";
        String insertSql = "INSERT INTO test7_1 VALUES (1,'Alice',true)";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.execute(insertSql);
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage().replaceAll("(?<=#)\\d+", ""))
                .isEqualTo("Error 90005 (00000) : Error while executing SQL \"INSERT INTO "
                    + "test7_1 VALUES (1,'Alice',true)\": Error while applying rule DingoValuesReduceRule(Project), "
                    + "args [rel#:LogicalProject.NONE(input=RelSubset#,exprs=[1, 'Alice', CAST(true):INTEGER NOT "
                    + "NULL]), rel#:LogicalValues.NONE(type=RecordType(INTEGER ZERO),tuples=[{ 0 }])]");
        }
    }

    // Insert Data Range Error
    @Test
    public void testColumnDataRangeError() throws SQLException {
        String createTableSql = "create table test8(id int, name varchar(256), age int, primary key(id))";
        String insertSql = "INSERT INTO test8 VALUES (1,'Alice', 2000000000000000)";
        String selectSql = "SELECT * from test8";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.execute(insertSql);
            ResultSet rs = statement.executeQuery(selectSql);
            while (rs.next()) {
                System.out.println("Result: ");
                System.out.println(rs.getString(1));
                System.out.println(rs.getString(2));
                System.out.println(rs.getString(3));
            }
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage().replaceAll("(?<=#)\\d+",""))
                .isEqualTo("Error 90005 (00000) : Error while executing SQL \"INSERT INTO test8 VALUES "
                    + "(1,'Alice', 2000000000000000)\": 2000000000000000 exceeds max 2147483647 or lower"
                    + " min value -2147483648");
        }
    }

    // Function not found
    @Test
    public void testNoFunction() throws SQLException {
        String createTableSql = "create table test9(id int, name varchar(256), age int, primary key(id))";
        String sql = "select  sum1(age) from test9";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeQuery(sql);
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage().replaceAll("(?<=#)\\d+",""))
                .isEqualTo("Error 90022 (00000) : Error while executing SQL "
                    + "\"select  sum1(age) from test9\": From line 1, column 9 to line 1, column 17: "
                    + "No match found for function signature SUM1(<NUMERIC>)");
        }
    }

    // Join name duplicated.
    @Test
    public void testJoinDuplicated() throws SQLException {
        String createTableSql = "create table test10(id int, name varchar(256), age int, primary key(id))";
        String sql = "select * from test10 as a join test10 as a on a.id = b.id";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeQuery(sql);
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage().replaceAll("(?<=#)\\d+",""))
                .isEqualTo("Error 90015 (00000) : Error while executing SQL \"select * from test10 as a join "
                    + "test10 as a on a.id = b.id\": From line 1, column 32 to line 1, column 42: Duplicate "
                    + "relation name 'A' in FROM clause");
        }
    }

    @Test
    public void testJoinNoCondition() throws SQLException {
        String createTableSql = "create table test11(id int, name varchar(256), age int, primary key(id))";
        String sql = "select * from test11 as a join test11 as b";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeQuery(sql);
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage().replaceAll("(?<=#)\\d+",""))
                .isEqualTo("Error 90016 (00000) : Error while executing SQL \"select * from test11 as a "
                    + "join test11 as b\": From line 1, column 27 to line 1, column 30: INNER, LEFT, RIGHT"
                    + " or FULL join requires a condition (NATURAL keyword or ON or USING clause)");
        }
    }

    @Test
    public void testJoinSelectColumnAmbiguous() throws SQLException {
        String createTableSql = "create table test12(id int, name varchar(256), age int, primary key(id))";
        String sql = "select name from test12 as a join test12 as b on a.id = b.id";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeQuery(sql);
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage().replaceAll("(?<=#)\\d+",""))
                .isEqualTo("Error 90017 (00000) : Error while executing SQL \"select name from test12 as a join"
                    + " test12 as b on a.id = b.id\": From line 1, column 8 to line 1, column 11:"
                    + " Column 'NAME' is ambiguous");
        }
    }

    @Test
    public void testInterpretError() throws SQLException {
        String sql = "select date_format('1999/33/01 01:01:01', '%Y/%m/%d %T')";
        try (Statement statement = connection.createStatement()) {
            statement.executeQuery(sql);
        } catch (Exception e) {
            System.out.println("Result: ");
            System.out.println(e.getMessage());
            assertThat(e.getMessage().replaceAll("(?<=#)\\d+",""))
                .isEqualTo("Error 90019 (00000) : Error while executing SQL \"select date_format('1999/33/01 01:01:01',"
                    + " '%Y/%m/%d %T')\": Error in parsing string \" Some parameters of the function are in the wrong "
                    + "format and cannot be parsed, error datetime: 1999/33/01 01:01:01\" to time, format is \"\".");
        }
    }

}
