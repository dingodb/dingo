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
import io.dingodb.sdk.operation.Value;
import io.dingodb.sdk.operation.filter.DingoFilter;
import io.dingodb.sdk.operation.filter.impl.DingoLogicalExpressFilter;
import io.dingodb.sdk.operation.filter.impl.DingoValueEqualsFilter;
import io.dingodb.sdk.client.DingoClient;
import io.dingodb.sdk.common.Key;
import io.dingodb.sdk.common.Record;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class DingoOperationExample {

    private static final String TABLE_SCHEMA = "Table schema: \n"
        + "| -------------- | ------------------- |\n"
        + "| ColumnName     | ColumnType          |\n"
        + "| -------------- | ------------------- |\n"
        + "| id             | id not null         |\n"
        + "| name           | varchar(20)         |\n"
        + "| age            | int                 |\n"
        + "| amount         | double              |\n"
        + "| data1          | array               |\n"
        + "| data2          | map                 |\n"
        + "| -------------- | ------------------- |\n";

    private static final String CREATE_SQL = "CREATE TABLE test("
        + "id int, "
        + "name varchar(20), "
        + "age int, "
        + "amount double, "
        + "data1 int ARRAY, "
        + "data2 MAP, PRIMARY KEY(id)"
        + ");";

    public static void main(String[] args) throws Exception {
        DingoClient dingoClient = new DingoClient("127.0.0.1:19181");
        dingoClient.open();

        String tableName = "TEST";
        int totalCnt = 100;
        if (args.length > 0) {
            tableName = args[0].toUpperCase();
            totalCnt = args.length > 1 ? Integer.parseInt(args[1]) : 100;
        }
        // ---------------------------- ----------------------------

        System.out.println(TABLE_SCHEMA);

        Random random = new Random();
        int bound = 20;
        List<Object[]> recordList = new ArrayList<>();
        for (int i = 0; i < totalCnt; i++) {
            Map<String, Integer> map = new HashMap<>();
            map.put("a", random.nextInt(bound));
            map.put("b", random.nextInt(bound));
            map.put("c", random.nextInt(bound));
            recordList.add(new Object[] {
                i,
                "name" + i,
                random.nextInt(bound),
                i * 0.1,
                Arrays.asList(random.nextInt(bound), random.nextInt(bound),
                    random.nextInt(bound), random.nextInt(bound), random.nextInt(bound)),
                map
            });
        }

        /*boolean isOK = */dingoClient.insert(tableName, recordList);
        /*if (!isOK) {
            System.exit(1);
        }*/

        Key startKey = new Key(tableName, ImmutableList.of(Value.get(0)));
        Key endKey = new Key(tableName, ImmutableList.of(Value.get(100)));

        List<Record> queryResult = dingoClient.query(startKey, endKey, null);
        for (Record record : queryResult) {
            System.out.println("query record ====> " + record.toString());
        }
        DingoFilter root = new DingoLogicalExpressFilter();
        DingoValueEqualsFilter eqFilter = new DingoValueEqualsFilter(new int[]{2}, new Object[]{18});
        root.addAndFilter(eqFilter);

        /*// count + filter
        List<DingoExecResult> countResult = dingoClient.count(startKey, endKey, root, new Column("name"));
        for (DingoExecResult cr : countResult) {
            System.out.println("count result ====> " + cr.getRecord());
        }

        // add
        boolean addResult = dingoClient.add(startKey, endKey, new Column("amount", 10.0));
        System.out.println("add result: " + addResult);

        // sum
        List<DingoExecResult> sumResult = dingoClient.sum(startKey, endKey, new Column("amount"));
        for (DingoExecResult sr : sumResult) {
            System.out.println("sum amount result ====> " + sr.get("amount"));
        }

        // sum + filter
        List<DingoExecResult> sumFilterResult = dingoClient.sum(startKey, endKey, root, new Column("amount"));
        for (DingoExecResult sfr : sumFilterResult) {
            System.out.println("sum amount filter result ====> " + sfr.get("amount"));
        }

        // max
        List<DingoExecResult> maxResult = dingoClient.max(startKey, endKey, new Column("amount"));
        for (DingoExecResult mr : maxResult) {
            System.out.println("max amount result ====> " + mr.get("amount"));
        }

        // max
        List<DingoExecResult> minResult = dingoClient.min(startKey, endKey, new Column("amount"));
        for (DingoExecResult mr : minResult) {
            System.out.println("min amount result ====> " + mr.get("amount"));
        }*/

        // collection
        /*Op arrSizeOp = new Op(
            CollectionType.SIZE, new BasicContext(new Column("data1")));
        Op getIndexOp = new Op(
            CollectionType.GET_BY_INDEX, new ListContext(2, new Column("data1")));
        Op getIndexRangeOp = new Op(
            CollectionType.GET_BY_INDEX_RANGE, new ListContext(1, 3, new Column("data1")));
        Op getAllOp = new Op(CollectionType.GET_ALL, new BasicContext(new Column("data1")));

        Op mapPut = new Op(
            CollectionType.PUT, new MapContext(Value.get("aa"), Value.get(5), new Column("data2")));

        Op mapGetByKey = new Op(
            CollectionType.GET_BY_KEY, new MapContext(Value.get("aa"), new Column("data2")));

        Op mapRemoveByKey = new Op(
            CollectionType.REMOVE_BY_KEY, new MapContext(Value.get("aa"), new Column("data2")));

        List<DingoExecResult> operateResult = dingoClient.operate(
            startKey,
            endKey,
            ImmutableList.of(arrSizeOp, getIndexOp, getIndexRangeOp, getAllOp, mapPut, mapGetByKey));
        for (DingoExecResult or : operateResult) {
            System.out.println("batch operate result, op ====> " + or.op() + ", record ====> " + or.getRecord());
        }*/

        dingoClient.dropTable(tableName);

        dingoClient.close();
    }
}
