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

package io.dingodb.example;

import com.google.common.collect.ImmutableList;
import io.dingodb.common.DingoOpResult;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.sdk.client.DingoClient;
import io.dingodb.sdk.common.Key;
import io.dingodb.sdk.common.Record;
import io.dingodb.sdk.operation.Column;
import io.dingodb.sdk.operation.Value;
import io.dingodb.sdk.operation.filter.DingoFilter;
import io.dingodb.sdk.operation.filter.impl.DingoGtFilter;
import io.dingodb.sdk.operation.filter.impl.DingoLogicalExpressFilter;
import io.dingodb.sdk.operation.filter.impl.DingoValueEqualsFilter;
import io.dingodb.sdk.operation.op.Op;
import io.dingodb.sdk.operation.result.CollectionOpResult;
import io.dingodb.sdk.operation.unit.numeric.NumberUnit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class DingoOperationExample {

    private static final String TABLE_SCHEMA = "Table schema: \n"
        + "| -------------- | ------------------- |\n"
        + "| ColumnName     | ColumnType          |\n"
        + "| -------------- | ------------------- |\n"
        + "| id             | id not null         |\n"
        + "| age            | int                 |\n"
        + "| amount         | int                 |\n"
        + "| -------------- | ------------------- |\n";

    private static final String CREATE_SQL = "CREATE TABLE test("
        + "id int, "
        + "age int, "
        + "amount int, "
        + "PRIMARY KEY(id)"
        + ");";

    public static void main(String[] args) throws Exception {
        DingoClient dingoClient = new DingoClient("127.0.0.1:19181");
        dingoClient.setIdentity("root", "123123");
        dingoClient.open();

        String tableName = "TEST";
        if (args.length > 0) {
            tableName = args[0].toUpperCase();
        }
        dingoClient.dropTable(tableName);
        // ---------------------------- ----------------------------

        System.out.println(TABLE_SCHEMA);

        TableDefinition definition = new TableDefinition(tableName);
        ColumnDefinition id = ColumnDefinition.builder().name("id").type("integer").primary(0).build();
        ColumnDefinition age = ColumnDefinition.builder().name("age").type("integer").build();
        ColumnDefinition amount = ColumnDefinition.builder().name("amount").type("integer").build();
        definition.setColumns(ImmutableList.of(id, age, amount));
        dingoClient.createTable(definition);
        dingoClient.insert(tableName,
            ImmutableList.of(
                new Object[]{1, 18, 77},
                new Object[]{2, 20, 78},
                new Object[]{3, 20, 77},
                new Object[]{4, 19, 76},
                new Object[]{5, 21, 75},
                new Object[]{6, 20, 100},
                new Object[]{7, 18, 80},
                new Object[]{8, 21, 79},
                new Object[]{9, 18, 78},
                new Object[]{10, 20, 67}
            ));

        Key start = new Key(tableName, new ArrayList<>(Collections.singleton(Value.get(1))));
        Key end = new Key(tableName, new ArrayList<>(Collections.singleton(Value.get(11))));

        List<Record> queryResult = dingoClient.query(start, end, null);
        for (Record record : queryResult) {
            System.out.println("query record ====> " + record.toString());
        }
        DingoFilter root = new DingoLogicalExpressFilter();
        DingoValueEqualsFilter eqFilter = new DingoValueEqualsFilter(new int[]{1}, new Object[]{20});
        root.addAndFilter(eqFilter);

        Op op = Op.scan(start, end).filter(root).sum(new Column("amount"));
        DingoOpResult result = dingoClient.exec(op);
        println(result, "filter + sum");

        op = Op.scan(start, end).decreaseCount(new Column("amount"));

        result = dingoClient.exec(op);
        println(result, "decrease");

        root = new DingoLogicalExpressFilter();
        DingoGtFilter gtFilter = new DingoGtFilter(3);
        root.addAndFilter(gtFilter);
        op = Op.scan(start, end).maxContinuousDecreaseCount(new Column("amount")).filter(root);
        result = dingoClient.exec(op);
        println(result, "max continuous decrease + filter");

        Key key = new Key(tableName, new ArrayList<>(Collections.singleton(Value.get(7))));

        Record getRecord = dingoClient.get(key);
        System.out.println("======= get record :" + getRecord + " ======");

        boolean delete = dingoClient.delete(key);
        System.out.println("======= delete isOk? " + delete + " =======");

        List<Record> query = dingoClient.query(start, end, null);
        for (Record record : query) {
            System.out.println("====== query record =====> " + record.toString());
        }

        dingoClient.dropTable(tableName);

        dingoClient.close();
    }

    private static void println(DingoOpResult result, String type) {
        System.out.println("======== " + type + " start ========");
        if (result instanceof CollectionOpResult) {
            Iterator<Object[]> iterator = ((CollectionOpResult<Iterator<Object[]>>) result).getValue();
            while (iterator.hasNext()) {
                System.out.println("===== iterator =====> " + Arrays.toString(iterator.next()));
            }
        } else {
            NumberUnit unit = (NumberUnit) result.getValue();
            System.out.println("===== number value: ======> " + unit.value());
        }
        System.out.println("======== " + type + " end ========");
    }
}
