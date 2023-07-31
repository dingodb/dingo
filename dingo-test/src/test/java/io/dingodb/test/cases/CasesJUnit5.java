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

package io.dingodb.test.cases;

import io.dingodb.calcite.DingoRootSchema;
import io.dingodb.test.dsl.Case;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.util.stream.Stream;

import static io.dingodb.test.dsl.Case.exec;
import static io.dingodb.test.dsl.Case.file;

public class CasesJUnit5 implements ArgumentsProvider {
    public static final String SELECT_ALL = "select * from {table}";

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        Stream<Arguments> stream = Stream.of(
            Case.of(
                "Create, insert and select",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec(SELECT_ALL)
                    .result(file("string_double/data.csv"))
            ),
            Case.of(
                "Table name with schema",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec("select * from " + DingoRootSchema.DEFAULT_SCHEMA_NAME + ".{table}")
                    .result(file("string_double/data.csv"))
            ),
            Case.of(
                "Select filtered",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec("select * from {table} where amount > 4.0")
                    .result(
                        "id, name, amount",
                        "INTEGER, STRING, DOUBLE",
                        "3, Cindy, 4.5",
                        "4, Doris, 5.0",
                        "5, Emily, 5.5",
                        "6, Alice, 6.0",
                        "7, Betty, 6.5",
                        "8, Alice, 7.0",
                        "9, Cindy, 7.5"
                    )
            ),
            Case.of(
                "Select projected",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec("select name as label, amount * 10.0 as score from {table}")
                    .result(
                        "label, score",
                        "STRING, DOUBLE",
                        "Alice, 35",
                        "Betty, 40",
                        "Cindy, 45",
                        "Doris, 50",
                        "Emily, 55",
                        "Alice, 60",
                        "Betty, 65",
                        "Alice, 70",
                        "Cindy, 75"
                    )
            ),
            Case.of(
                "Get by primary key",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec("select * from {table} where id = 1")
                    .result(
                        "id, name, amount",
                        "INT, STRING, DOUBLE",
                        "1, Alice, 3.5"
                    )
            ),
            Case.of(
                "Get by primary key 1",
                exec(file("misc/create.sql")),
                exec(file("misc/data.sql")),
                exec("select * from {table} where id = 1")
                    .result(
                        "id,name,age,gmt,price,amount,address,birthday,create_time,update_time,zip_code,is_delete",
                        "INT,STRING,INT,LONG,DOUBLE,DOUBLE,STRING,DATE,STRING,TIMESTAMP,STRING,BOOL",
                        "1,zhangsan,18,99,0.0,23.5,beijing,1998-04-06,08:10:10,2022-04-08 18:05:07,null,true"
                    )
            ),
            Case.of(
                "Get by `or` of primary key",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec("select * from {table} where id = 1 or id = 2")
                    .result(
                        "id, name, amount",
                        "INTEGER, STRING, DOUBLE",
                        "1, Alice, 3.5",
                        "2, Betty, 4.0"
                    )
            ),
            Case.of(
                "Get by `in list` of primary key",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec("select * from {table} where id in (1, 2, 3)")
                    .result(
                        "id, name, amount",
                        "INTEGER, STRING, DOUBLE",
                        "1, Alice, 3.5",
                        "2, Betty, 4.0",
                        "3, Cindy, 4.5"
                    )
            ),
            Case.of(
                "Multiple primary keys",
                exec(file("string_double/create_with_multi_keys.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec("select * from {table} where name between 'Betty' and 'Cindy'")
                    .result(
                        "id, name, amount",
                        "INT, STRING, DOUBLE",
                        "2, Betty, 4.0",
                        "3, Cindy, 4.5",
                        "7, Betty, 6.5",
                        "9, Cindy, 7.5"
                    )
            ),
            Case.of(
                "Mismatched type in expression",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec("select * from {table} where amount < 2147483648")
                    .result(file("string_double/data.csv"))
            ),
            Case.of(
                "Select filtered by `not in list` of primary key",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec("select * from {table} where id not in (3, 4, 5, 6, 7, 8, 9)")
                    .result(
                        "id, name, amount",
                        "INTEGER, STRING, DOUBLE",
                        "1, Alice, 3.5",
                        "2, Betty, 4.0"
                    )
            ),
            Case.of(
                "Select filtered by `and` of conditions",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec("select * from {table} where id > 1 and name = 'Alice' and amount > 6")
                    .result(
                        "id, name, amount",
                        "INTEGER, STRING, DOUBLE",
                        "8, Alice, 7.0"
                    )
            ),
            Case.of(
                "With null",
                exec(file("double/create.sql")),
                exec(file("double/data_with_null.sql")).updateCount(2),
                exec(SELECT_ALL)
                    .result(file("double/data_with_null_all.csv"))
            ),
            Case.of(
                "Cast double to int",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec("select id, name, cast(amount as int) as amount from {table}")
                    .result(
                        "id, name, amount",
                        "INTEGER, STRING, INTEGER",
                        "1, Alice, 4",
                        "2, Betty, 4",
                        "3, Cindy, 5",
                        "4, Doris, 5",
                        "5, Emily, 6",
                        "6, Alice, 6",
                        "7, Betty, 7",
                        "8, Alice, 7",
                        "9, Cindy, 8"
                    )
            ),
            Case.of(
                "Cast int to boolean",
                exec(file("boolean/create.sql")),
                exec(file("boolean/data_of_int.sql")).updateCount(2),
                exec(file("boolean/select_true.sql"))
                    .result(file("boolean/data_of_true.csv"))
            ),
            Case.of(
                "Double as primary key",
                exec(file("double_pm/create.sql")),
                exec(file("double_pm/data.sql")).updateCount(3),
                exec(SELECT_ALL)
                    .result(file("double_pm/data.csv"))
            ),
            Case.of(
                "Date as primary key",
                exec(file("date_key/create.sql")),
                exec(file("date_key/data.sql")).updateCount(2),
                exec(SELECT_ALL)
                    .result(file("date_key/data_all.csv"))
            ),
            Case.of(
                "Date",
                exec(file("string_date/create.sql")),
                exec(file("string_date/data.sql")).updateCount(2),
                exec(SELECT_ALL)
                    .result(file("string_date/data.csv"))
            ),
            Case.of(
                "Time",
                exec(file("string_time/create.sql")),
                exec(file("string_time/data.sql")).updateCount(2),
                exec(SELECT_ALL)
                    .result(file("string_time/data.csv"))
            ),
            Case.of(
                "Timestamp",
                exec(file("string_timestamp/create.sql")),
                exec(file("string_timestamp/data.sql")).updateCount(2),
                exec(SELECT_ALL)
                    .result(file("string_timestamp/data.csv"))
            ),
            Case.of(
                "Concat null",
                exec(file("strings/create.sql")),
                exec(file("strings/data_with_null.sql")).updateCount(1),
                exec(file("strings/select_concat_all.sql"))
                    .result(file("strings/data_with_null_concat_all.csv"))
            ),
            Case.of(
                "Concat date",
                exec(file("string_date/create.sql")),
                exec(file("string_date/data.sql")).updateCount(2),
                exec("select 'test-' || birth from {table} where id = 1")
                    .result(
                        "EXPR$0",
                        "STRING",
                        "test-2020-01-01"
                    )
            ),
            Case.of(
                "Concat time",
                exec(file("string_time/create.sql")),
                exec(file("string_time/data.sql")).updateCount(2),
                exec("select 'test-' || birth from {table} where id = 1")
                    .result(
                        "EXPR$0",
                        "STRING",
                        "test-00:00:01"
                    )
            ),
            Case.of(
                "Concat int-string-time",
                exec(file("string_time/create.sql")),
                exec(file("string_time/data.sql")).updateCount(2),
                exec("select id || name || birth from {table} where id = 1")
                    .result(
                        "EXPR$0",
                        "STRING",
                        "1Alice00:00:01"
                    )
            ),
            Case.of(
                "Function `case`",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec(file("string_double/select_case_when.sql"))
                    .result(file("string_double/select_case_when.csv"))
            ),
            Case.of(
                "Function `case` with multiple `when`",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")).updateCount(9),
                exec(file("string_double/select_case_when_1.sql"))
                    .result(file("string_double/select_case_when_1.csv"))
            ),
            Case.of(
                "Function `pow`",
                exec(file("string_int_double/create.sql")),
                exec(file("string_int_double/data.sql")).updateCount(2),
                exec(file("string_int_double/select_pow_all.sql"))
                    .result(file("string_int_double/data_pow_all.csv")),
                exec(file("string_int_double/select_mod_all.sql"))
                    .result(file("string_int_double/data_mod_all.csv"))
            ),
            Case.of(
                "Map",
                exec(file("string_int_double_map/create.sql")),
                exec(file("string_int_double_map/data.sql")).updateCount(1),
                exec(file("string_int_double_map/update.sql")).updateCount(1),
                exec(file("string_int_double_map/select_scalar.sql"))
                    .result(file("string_int_double_map/data_scalar.csv"))
            ),
            Case.of(
                "Array",
                exec(file("array/create.sql")),
                exec(file("array/data.sql")),
                exec(file("array/select_array_item_all.sql"))
                    .result(file("array/data_array_item_all.csv"))
            ),
            Case.of(
                "Update using function",
                exec(file("string_int_double_string/create.sql")),
                exec(file("string_int_double_string/data.sql")).updateCount(9),
                exec(file("string_int_double_string/update.sql")).updateCount(2),
                exec(SELECT_ALL)
                    .result(file("string_int_double_string/data_updated.csv"))
            ),
            // In list with >= 20 elements is converted as join with values.
            Case.of(
                "In list with >=20 elements",
                exec(file("string_int_double_string/create.sql")),
                exec(file("string_int_double_string/data.sql")).updateCount(9),
                exec(file("string_int_double_string/update_1.sql")).updateCount(9),
                exec(SELECT_ALL)
                    .result(file("string_int_double_string/data_updated_1.csv"))
            ),
            Case.of(
                "Select with conflicting conditions",
                exec(file("string_double/create.sql")),
                exec(file("string_double/select_conflict.sql"))
                    .result(
                        "ID, NAME, AMOUNT",
                        "INT, STRING, DOUBLE"
                    )
            ),
            Case.of(
                "Delete with conflicting conditions",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")),
                exec("delete from {table} where name = 'Alice' and name = 'Betty'").updateCount(0),
                exec(SELECT_ALL).result(file("string_double/data.csv"))
            ),
            Case.of(
                "Update with conflicting conditions",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")),
                exec("update {table} set amount = 0.0 where name = 'Alice' and name = 'Betty'").updateCount(0),
                exec(SELECT_ALL).result(file("string_double/data.csv"))
            ),
            Case.of(
                "Select nothing",
                exec(file("float_date_timestamp/create.sql")),
                exec(file("float_date_timestamp/data.sql")),
                exec(
                    "select * from {table} where card_no=23 and `account`=14"
                ).result(
                    "ID,CARD_NO,NAME,ACCOUNT,TIME_DATE,TIME_DATETIME",
                    "INT,INT,STRING,FLOAT,DATE,TIMESTAMP"
                )
            ),
            Case.of(
                "Select by timestamp = now()",
                exec(file("float_date_timestamp/create.sql")),
                exec(file("float_date_timestamp/data.sql")),
                exec(
                    "select * from {table} where time_datetime=now()"
                ).result(
                    "ID,CARD_NO,NAME,ACCOUNT,TIME_DATE,TIME_DATETIME",
                    "INT,INT,STRING,FLOAT,DATE,TIMESTAMP"
                )
            ),
            Case.of(
                "Insert int to long",
                exec(file("long_double/create.sql")),
                exec(file("long_double/data.sql")),
                exec(SELECT_ALL).result(
                    "ID,AMT,AMOUNT",
                    "INT,LONG,DOUBLE",
                    "1,55,23.45"
                )
            ),
            Case.of(
                "Insert int to long key",
                exec(file("long_key/create.sql")),
                exec(file("long_key/data.sql")),
                exec(SELECT_ALL).result(
                    "ID,AMT,AMOUNT",
                    "LONG,LONG,DOUBLE",
                    "1,55,23.45"
                )
            ),
            Case.of(
                "Update long with init",
                exec(file("long_double/create.sql")),
                exec(file("long_double/data.sql")),
                exec("update {table} set amt=15 where id = 1"),
                exec(SELECT_ALL).result(
                    "ID,AMT,AMOUNT",
                    "INT,LONG,DOUBLE",
                    "1,15,23.45"
                )
            ),
            Case.of(
                "Count of empty table",
                exec(file("string_double/create.sql")),
                exec("select count(amount) from {table}")
                    .result(
                        "EXPR$0",
                        "LONG",
                        "0"
                    )
            ),
            Case.of(
                "Aggregation of empty table",
                exec(file("string_double/create.sql")),
                exec("select sum(id) as `sum`, avg(amount) as `avg` from {table}")
                    .result(
                        "SUM,AVG",
                        "INT,DOUBLE",
                        "NULL,NULL"
                    )
            ),
            Case.of(
                "Count group by bool",
                exec(file("int_string_double_boolean/create.sql")),
                exec(file("int_string_double_boolean/data.sql")),
                exec("select is_delete, count(*) from {table} group by is_delete")
                    .result(
                        "IS_DELETE,EXPR$1",
                        "BOOL,LONG",
                        "true,3",
                        "false,3",
                        "null,2"
                    )
            ),
            Case.of(
                "Aggregation group by",
                exec(file("string_double/create.sql")),
                exec(file("string_double/data.sql")),
                exec("select name, sum(amount) as `sum` from {table} group by name")
                    .result(file("string_double/select_sum_group_by.csv"))
            ),
            Case.of(
                "Aggregation",
                exec(file("misc/create.sql")),
                exec(file("misc/data.sql")),
                exec("select avg(age) aa, min(amount) ma, address from {table}" +
                    " where id in (1,3,5,7,9,13,35) or name<>'zhangsan' group by address order by ma limit 2")
                    .result(
                        "AA, MA, ADDRESS",
                        "INT, DOUBLE, STRING",
                        "544, 0.0, 543",
                        "76, 2.3, beijing changyang"
                    )
            ),
            Case.of(
                "Aggregation 1",
                exec(file("misc/create.sql")),
                exec(file("misc/data.sql")),
                exec("select address, sum(amount) sa from {table} where address between 'C' and 'c' group by address")
                    .result(
                        "address, sa",
                        "STRING, DOUBLE",
                        "beijing changyang, 2.3",
                        "beijing, 23.5",
                        "CHANGping, 9.0762556"
                    )
            ),
            Case.of(
                "Root selection",
                exec("create table {table}(name varchar(32) not null, age int, amount double, primary key(name))"),
                exec("insert into {table} values\n" +
                    "('Alice', 18, 3.5),\n" +
                    "('Betty', 22, 4.1),\n" +
                    "('Cindy', 39, 4.6),\n" +
                    "('Doris', 25, 5.2),\n" +
                    "('Emily', 24, 5.8)"),
                exec("select name from {table} order by age").result(
                    "name",
                    "STRING",
                    "Alice",
                    "Betty",
                    "Emily",
                    "Doris",
                    "Cindy"
                )
            ),
            Case.of(
                "new test",
                exec("CREATE TABLE {table} (\n" +
                    "    id int,\n" +
                    "    name varchar(64),\n" +
                    "    age int,\n" +
                    "    amount DOUBLE,\n" +
                    "    address varchar(255),\n" +
                    "    birthday DATE,\n" +
                    "    create_time TIME,\n" +
                    "    update_time TIMESTAMP,\n" +
                    "    is_delete boolean,\n" +
                    "    PRIMARY KEY (id)\n" +
                    ") "
                ),
                exec("insert into {table} values\n" +
                    "(1,'zhangsan',18,23.50,'beijing','1998-04-06','08:10:10','2022-04-08 18:05:07', true),\n" +
                    "(2,'lisi',25,895,' beijing haidian ','1988-02-05','06:15:08','2000-02-29 00:00:00', false),\n" +
                    "(3,'l3',55,123.123,'wuhan NO.1 Street','2022-03-04','07:03:15','1999-02-28 23:59:59', false),\n" +
                    "(4,'HAHA',57,9.0762556,'CHANGping', '2020-11-11', '05:59:59', '2021-05-04 12:00:00', True),\n" +
                    "(5,'awJDs',1,1453.9999,'pingYang1', '2010-10-01', '19:00:00', '2010-10-01 02:02:02', TRUE),\n" +
                    "(6,'123',544,0,'543', '1987-07-16', '01:02:03', '1952-12-31 12:12:12', true),\n" +
                    "(7,'yamaha',76,2.30,'beijing changyang', '1949-01-01', '00:30:08', '2022-12-01 01:02:03', False),\n" +
                    "(8,'zhangsan',18,12.3,'shanghai','2015-09-10', '03:45:10', '2001-11-11 18:05:07', true),\n" +
                    "(9,'op ',76,109.325,'wuhan', '1995-12-15', '16:35:38', '2008-08-08 08:00:00', true),\n" +
                    "(10,'lisi',256,1234.456,'nanjing', '2021-03-04', '17:30:15', '1999-02-28 00:59:59', FALSE),\n" +
                    "(11,'  aB c  dE ',61,99.9999,'beijing chaoyang', '1976-07-07', '06:00:00', '2024-05-04 12:00:00', false),\n" +
                    "(12,' abcdef',2,2345.000,'123', '2018-05-31', '21:00:00', '2000-01-01 00:00:00', true),\n" +
                    "(13,'HAHA',57,9.0762556,'CHANGping', '2014-10-13', '01:00:00', '1999-12-31 23:59:59', false),\n" +
                    "(14,'zhngsna',99,32,'chong qing ', '1949-10-01', '12:30:00', '2022-12-31 23:59:59', true),\n" +
                    "(15,'1.5',18,0.1235,'http://WWW.baidu.com','2007-08-15', '22:10:10', '2020-02-29 05:53:44', true),\n" +
                    "(16,' ',82,1999.99,'Huluodao', '1960-11-11', '14:09:49', '2000-02-29 00:00:00', false),\n" +
                    "(17,'',0,0.01,null, '2022-03-01', '15:20:20', '1953-10-21 16:10:28', true),\n" +
                    "(18,'tTATtt',181,18.18,' aabcaa ', '2020-11-11', '05:59:59', '2021-05-04 12:00:00', false),\n" +
                    "(19,'777',77,77.77,'7788', '2020-11-11', '05:59:59', '2021-05-04 12:00:00', false),\n" +
                    "(20,null,null,null,null, '1987-12-11', '11:11:00', '1997-07-01 00:00:00', null),\n" +
                    "(21,'Zala',76,2000.01,'JiZhou', '2022-07-07', '00:00:00', '2022-07-07 13:30:03', false)"),
                exec("select id,name,is_delete from {table} where is_delete in (false)")
            )
        );
        return stream;//.filter(arg -> arg.get()[0].equals("new test"));
    }
}
