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

package io.dingodb.test.datefuc;

import io.dingodb.exec.Services;
import io.dingodb.meta.test.MetaTestService;
import io.dingodb.test.SqlHelper;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class InsertAndSelectDateTest {
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

    //Result like: 2022-03-30 02:19:42
    @Test
    public void testInsertDateAndFormatOutput() throws SQLException {
        String createTableSql =  "create table test("
            + "id int,"
            + "name varchar(32) not null,"
            + "age int,"
            + "amount double,"
            + "address varchar(255),"
            + "birthday date,"
            + "createTime time,"
            + "update_Time timestamp,"
            + "primary key(id)"
            + ")";

        String batInsertSql = "insert into test"
            + " values "
            + "(1,'zhangsan',18,23.50,'beijing','20220401', '08:10:10', '2022-4-8 18:05:07') ,"
            + "(2,'lisi',25,895,' beijing haidian ', '1988-2-05', '06:15:8', '2000-02-29 00:00:00') ,"
            + " (3,'l3',55,123.123,'wuhan NO.1 Street', '2022-03-4', '07:3:15', '1999-2-28 23:59:59') ,"
            + "(4,'HAHA',57,9.0762556,'CHANGping', '2020-11-11', '5:59:59', '2021-05-04 12:00:00') ,"
            + "(5,'awJDs',1,1453.9999,'pingYang1', '2010-10-1', '19:0:0', '2010-10-1 02:02:02') ,"
            + "(6,'123',544,0,'543', '1987-7-16', '1:2:3', '1952-12-31 12:12:12'),"
            + "(7,'yamaha',76,2.30,'beijing changyang', '1949-01-01', '0:30:8', '2022-12-01 1:2:3')";

        //String queryDFTSSql = "select name, unix_timestamp(birthday) ts_out from test";
        //String queryDFTSSql = "select name, time_format(createTime, '%H:%i:%s') ts_out from test where id<8";
        // String queryDFTSSql = "select name, createTime  from test where id<8";
        String queryDFTSSql = "select name,date_format(update_time, '%Y/%m/%d %H.%i.%s') ts_out from test where id=1";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(batInsertSql);
            try (ResultSet rs = statement.executeQuery(queryDFTSSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    System.out.println(rs.getString(2));
                    assertThat(rs.getString(2)).isEqualTo("2022/04/08 18.05.07");
                    System.out.println(rs.getString(2));
                }
            }
        }
    }

    @Test
    public void testInsertTime() throws SQLException {
        String createTableSql = "create table timetest(id int, name varchar(20), age int, "
            + "amount double, create_time time, primary key (id))";
        String insertSql = "insert into timetest values(1, 'zhang san', 18, 1342.09, '112341')";
        String selectSql = "SELECT * from timetest";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            try (ResultSet rs = statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    System.out.println(rs.getString(2));
                    System.out.println(rs.getString(5));
                    assertThat(rs.getString(5)).isEqualTo("11:23:41");
                    //System.out.println(rs.getString(3));
                }
            }
        }
    }

    @Test
    @Disabled
    public void testInsertTime1() throws SQLException {
        String createTableSql = "create table timetest1(id int, name varchar(20), age int, "
            + "amount double, create_time time, primary key (id))";
        String insertSql = "insert into timetest1 values(1, 'zhang san', 18, 1342.09, '4:00:62')";
        String selectSql = "SELECT * from timetest1";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            try (ResultSet rs = statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    System.out.println(rs.getString(2));
                    // assertThat(rs.getString(2)).isEqualTo("2022/04/08 18.05.07");
                    //System.out.println(rs.getString(3));
                }
            }
        }
    }

    @Test
    @Disabled
    public void testInsertTime2() throws SQLException {
        String createTableSql = "create table timetest2(id int, name varchar(20), age int, "
            + "amount double, create_time time, primary key (id))";
        String insertSql = "insert into timetest2 values(1, 'zhang san', 18, 1342.09, '4:60:00')";
        String selectSql = "SELECT * from timetest2";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            try (ResultSet rs = statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    System.out.println(rs.getString(2));
                    // assertThat(rs.getString(2)).isEqualTo("2022/04/08 18.05.07");
                    //System.out.println(rs.getString(3));
                }
            }
        }
    }

    @Test
    public void testInsertTime3() throws SQLException {
        String createTableSql = "create table timetest3(id int, name varchar(20), age int, amount double,"
            + " address varchar(255),update_time time,primary key (id))";
        String insertSql = "insert into timetest3 values(1,'aa',18,2.5,'beijing','17:38:28')";
        String selectSql = "select name,time_format(update_time, '%H:%i:%s') time_out from timetest3";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            try (ResultSet rs = statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    System.out.println(rs.getString(2));
                    assertThat(rs.getString(2)).isEqualTo("17:38:28");
                }
            }
        }
    }

    @Test
    public void testInsertTime4() throws SQLException {
        String createTableSql = "create table dateinserttest4(id int, name varchar(20), age int, amount double,"
            + " address varchar(255), birthday date, create_time time, update_time timestamp, "
            + "is_delete boolean, primary key (id))";

        String insertSql = "insert into dateinserttest4 values "
            + "(1,'zhangsan',18,23.50,'beijing','1998-4-6', '08:10:10', '2022-4-8 18:05:07', true),"
            + "(2,'lisi',25,895,' beijing haidian ', '1988-2-05', '06:15:8', '2000-02-29 00:00:00', false),"
            + "(3,'l3',55,123.123,'wuhan NO.1 Street', '2022-03-4', '07:3:15', '1999-2-28 23:59:59', true),"
            + "(4,'HAHA',57,9.0762556,'CHANGping', '2020-11-11', '5:59:59', '2021-05-04 12:00:00', true),"
            + "(5,'awJDs',1,1453.9999,'pingYang1', '2010-10-1', '19:0:0', '2010-10-1 02:02:02', false),"
            + "(6,'123',544,0,'543', '1987-7-16', '1:2:3', '1952-12-31 12:12:12', false),"
            + "(7,'yamaha',76,2.30,'beijing changyang', '1949-01-01', '0:30:8', '2022-12-01 1:2:3', true)";
        // 19:00:00
        String selectSql = "select max(create_time) from dateinserttest4";
        // 00:30:08
        // String selectSql = "select min(create_time) from dateinserttest4";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            try (ResultSet rs = statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("19:00:00");
                }
            }
        }
    }

    @Test
    @Disabled
    public void testInsertTime5() throws SQLException {
        String createTableSql = "create table timetest5(id int, name varchar(20), age int, amount double,"
            + " address varchar(255),update_time time,primary key (id))";
        String insertSql = "insert into timetest5 values(1,'aa',18,2.5,'beijing','24:00:00')";
        String selectSql = "select name,time_format(update_time, '%H:%i:%s') time_out from timetest5";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            try (ResultSet rs = statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    System.out.println(rs.getString(2));
                }
            }
        }
    }

    @Test
    public void testInsertAndSerialize() throws SQLException {
        String createTableSql = "create table test22(id int, name varchar(20), age int, "
            + "amount double, primary key(id))";
        String insertSql = "insert into test22 values(1,'zhangsan',16,20.35)";
        String selectSql = "SELECT * from test22";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            try (ResultSet rs = statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    System.out.println(rs.getString(3));
                    assertThat(rs.getString(3)).isEqualTo("16");
                }
            }
        }
    }

    @Test
    public void testInsertAndSerialize1() throws SQLException {
        String createTableSql = "create table strdemo1(id int, name varchar(32) not null, age int,"
            + " address varchar(255), primary key(id))";
        String insertSql = "insert into strdemo1 values(1,'zhangsan',18,'beijing')";
        String selectSql = "SELECT * from strdemo1";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            try (ResultSet rs = statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    System.out.println(rs.getString(3));
                    assertThat(rs.getString(3)).isEqualTo("18");
                }
            }
        }
    }

    @Test
    public void testInsertAndSerialize2() throws SQLException {
        String createTableSql = "create table testnull(id int, name varchar(20), age int, primary key(id))";
        String insertSql = "insert into testnull(id,name) values (1,'zhangsan')";
        String selectSql = "SELECT * from testnull";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            try (ResultSet rs = statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    assertThat(rs.getString(1)).isEqualTo("1");
                }
            }
        }
    }

    @Test
    @Disabled
    public void testInsertAndSerialize3() throws SQLException {
        String createTableSql = "create table defect0033(id int, name varchar(20) not null, age int not null, "
            + "amount double, address varchar(255), birthday date, create_time time, update_time timestamp,"
            + " is_delete boolean, primary key (id))";

        String insertSql = "insert into defect0033 values "
            //+ "(9,'Wula',71,98.20, 'shanghai',null,'23:59:59','2022-06-13 18:36:35',0),"
            //+ "(10,'Wula',71,98.20,'shanghai','1900-01-01',null,'2022-06-13 18:36:35',1),"
            + "(11,'Wula',71,98.20,'shanghai','2000-12-31','00:00:00',null,2)";
        String selectSql = "SELECT * from defect0033";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            try (ResultSet rs = statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    //assertThat(rs.getString(1)).isEqualTo("1");
                }
            }
        }
    }

    @Test
    public void testInsertDate() throws SQLException {
        String createTableSql = "create table datetest(id int, name varchar(20), age int, "
            + "amount double, create_date date, primary key (id))";
        String insertSql = "insert into datetest values(1, 'zhang san', 18, 1342.09, '2020.01.01')";
        String selectSql = "SELECT * from datetest";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            try (ResultSet rs = statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(4));
                    System.out.println(rs.getString(5));
                    assertThat(rs.getString(5)).isEqualTo("2020-01-01");
                }
            }
        }
    }

    @Test
    public void testInsertDate1() throws SQLException {
        String createTableSql = "create table datetest1(id int, name varchar(20), age int, "
            + "amount double, create_date date, primary key (id))";
        String insertSql = "insert into datetest1 values(1, 'zhang san', 18, 1342.09, '1949-01-01')";
        String selectSql = "SELECT * from datetest1";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            try (ResultSet rs = statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(4));
                    System.out.println(rs.getString(5));
                    //assertThat(rs.getString(2)).isEqualTo("2022/04/08 18.05.07");
                    //System.out.println(rs.getString(3));
                }
            }
        }
    }

    @Test
    public void testInsertDate2() throws SQLException {
        String createTableSql = "create table datetest2(id int, name varchar(20), age int, "
            + "amount double, create_date date, primary key (id))";
        String insertSql = "insert into datetest2 values(1, 'zhang san', 18, 1342.09, '2020/4/1')";
        String selectSql = "SELECT * from datetest2";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            try (ResultSet rs = statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(4));
                    System.out.println(rs.getString(5));
                    assertThat(rs.getString(5)).isEqualTo("2020-04-01");
                    //System.out.println(rs.getString(3));
                }
            }
        }
    }

    @Test
    @Disabled
    public void testInsertDate3() throws SQLException {
        String createTableSql = "create table datetest3(id int, name varchar(20), age int, "
            + "amount double, create_date date, primary key (id))";
        String insertSql = "insert into datetest3 values(1, 'zhang san', 18, 1342.09, '88-11-11')";
        String selectSql = "SELECT * from datetest3";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            try (ResultSet rs = statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(4));
                    System.out.println(rs.getString(5));
                    assertThat(rs.getString(5)).isEqualTo("2022-04-01");
                    //System.out.println(rs.getString(3));
                }
            }
        }
    }

    @Test
    public void testInsertDate4() throws SQLException {
        String createTableSql = "create table datetest4(id int, n int, name varchar(20), age int, "
            + "amount double, create_date date, primary key (id, n))";
        String insertSql = "insert into datetest4 values(1, 2, 'zhang san', 18, 1342.09, '2020/4/1')";
        String selectSql = "SELECT * from datetest4";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            try (ResultSet rs = statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(4));
                    System.out.println(rs.getString(5));
                    assertThat(rs.getString(6)).isEqualTo("2020-04-01");
                    //System.out.println(rs.getString(3));
                }
            }
        }
    }

    @Test
    public void testInsertDate5() throws SQLException {
        String createTableSql = "create table datetest5(id int, birthday date,primary key(id))";
        String insertSql = "insert into datetest5 values(1,'2010-10-01')";
        String selectSql = "SELECT max(birthday) from datetest5";

        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            try (ResultSet rs = statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    //System.out.println(rs.getString(2));
                    assertThat(rs.getString(1)).isEqualTo("2010-10-01");
                    //System.out.println(rs.getString(3));
                }
            }
        }
    }

    @Test
    public void testInsertDate6() throws SQLException {
        String createTableSql = "create table datetest6(id int, birthday date,primary key(id))";
        String insertSql = "insert into datetest6 values(1,'2022-03-04')";
        //String selectSql = "SELECT * from datetest5";
        String selectSql = "SELECT max(birthday) from datetest6";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            try (ResultSet rs = statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                    //System.out.println(rs.getString(2));
                    assertThat(rs.getString(1)).isEqualTo("2022-03-04");
                    //System.out.println(rs.getString(3));
                }
            }
        }
    }

    @Test
    public void testInsertTimestamp1() throws SQLException {
        String createTableSql = "create table tstest(id int, create_time time, update_time timestamp,primary key(id))";
        String insertSql = "insert into tstest values(1,'14:00:00','2022-06-22 10:10:59'),"
            + "(2,'01:13:06','1949-10-01 01:00:00'),(3,'23:59:59','1987-07-16 00:00:00')";
        String selectSql = "SELECT * from tstest where id = 2";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSql);
            statement.executeUpdate(insertSql);
            try (ResultSet rs = statement.executeQuery(selectSql)) {
                System.out.println("Result: ");
                while (rs.next()) {
                    System.out.println(rs.getString(2));
                    System.out.println(rs.getString(3));
                    assertThat(rs.getString(3)).isEqualTo("1949-10-01 01:00:00");
                    //System.out.println(rs.getString(3));
                }
            }
        }
    }

}
