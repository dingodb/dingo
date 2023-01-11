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

package io.dingodb.example.index;

import io.dingodb.common.CommonId;
import io.dingodb.common.environment.ExecutionEnvironment;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.meta.MetaService;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.server.api.ExecutorApi;
import io.dingodb.server.api.TableApi;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.client.connector.impl.ServiceConnector;
import io.dingodb.server.client.meta.service.MetaServiceClient;
import io.dingodb.server.executor.index.IndexExecutor;

import java.util.ArrayList;
import java.util.List;

public class IndexExample {

    private static String tableName = "TEST";
    private static String coordinatorAddress = "localhost:19181";


    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setInfo("user", "root");
        env.setInfo("password", "123123");

        // 1.1 Init TableApi
        MetaService metaServiceClient = new MetaServiceClient(CoordinatorConnector.getCoordinatorConnector(coordinatorAddress));

        // 1.2 CreateTable
        TableDefinition testTable = getTableDefinition();
        metaServiceClient = metaServiceClient.getSubMetaService("DINGO");
        metaServiceClient.createTable(tableName, testTable);
        printInfo(metaServiceClient);

        // 1.3 init tableApi
        CommonId tableId = metaServiceClient.getTableId(tableName);
        ServiceConnector serviceConnector = ((MetaServiceClient) metaServiceClient).getTableConnector(tableId);
        TableApi tableApi = ApiRegistry.getDefault().proxy(TableApi.class, serviceConnector);

        // 1.4 Init ExecutorApi
        ExecutorApi executorcApi = ApiRegistry.getDefault().proxy(ExecutorApi.class, serviceConnector);

        // 2.1 Prepare index
        Index in1 = getIndexIn1();
        Index in2 = getIndexIn2();

        // 2.2 Prepare demo data
        List<Object[]> data1 = getData1();
        List<Object[]> data2 = getData2();
        List<Object[]> data3 = getData3();
        List<Object[]> selectIn1Data1 = getSelectByIn1Data1();
        List<Object[]> selectIn2Data1 = getSelectByIn2Data1();
        List<Object[]> updateData = getUpdateData();
        List<Object[]> deleteData = getDeleteData();
        List<Object[]> selectIn1Data2 = getSelectByIn1Data2();
        List<Object[]> selectIn2Data2 = getSelectByIn2Data2();


        // 3. insert data1
        boolean i1 = executorcApi.insert(tableId, data1.get(0));
        boolean i2 = executorcApi.insert(tableId, data1.get(1));
        boolean i3 = executorcApi.insert(tableId, data1.get(2));
        boolean i4 = executorcApi.insert(tableId, data1.get(3));
        boolean i5 = executorcApi.insert(tableId, data1.get(4));
        printInfo(metaServiceClient);

        // 4. add index in1
        CommonId indexIn1Id = tableApi.createIndex(tableId, in1);
        printInfo(metaServiceClient);

        // 5. insert data2
        boolean i6 = executorcApi.insert(tableId, data2.get(0));
        boolean i7 = executorcApi.insert(tableId, data2.get(1));
        boolean i8 = executorcApi.insert(tableId, data2.get(2));
        boolean i9 = executorcApi.insert(tableId, data2.get(3));
        boolean i10 = executorcApi.insert(tableId, data2.get(4));
        printInfo(metaServiceClient);

        // 6. add index in2
        CommonId indexIn2Id = tableApi.createIndex(tableId, in2);
        printInfo(metaServiceClient);

        // 7. insert data3
        boolean i11 = executorcApi.insert(tableId, data3.get(0));
        boolean i12 = executorcApi.insert(tableId, data3.get(1));
        boolean i13 = executorcApi.insert(tableId, data3.get(2));
        boolean i14 = executorcApi.insert(tableId, data3.get(3));
        boolean i15 = executorcApi.insert(tableId, data3.get(4));
        printInfo(metaServiceClient);

        // 8. select by in1
        List<Object[]> r11 = executorcApi.select(tableId, selectIn1Data1.get(0), new boolean[]{false, true, true, true, false});
        List<Object[]> r12 = executorcApi.select(tableId, selectIn1Data1.get(1), new boolean[]{false, true, true, true, false});
        List<Object[]> r13 = executorcApi.select(tableId, selectIn1Data1.get(2), new boolean[]{false, true, true, true, false});

        // 9. select by in2
        List<Object[]> r21 = executorcApi.select(tableId, selectIn2Data1.get(0), new boolean[]{false, true, true, false, true});
        List<Object[]> r22 = executorcApi.select(tableId, selectIn2Data1.get(1), new boolean[]{false, true, true, false, true});
        List<Object[]> r23 = executorcApi.select(tableId, selectIn2Data1.get(2), new boolean[]{false, true, true, false, true});

        // 10. update
        boolean u1 = executorcApi.update(tableId, updateData.get(0));
        boolean u2 = executorcApi.update(tableId, updateData.get(1));
        boolean u3 = executorcApi.update(tableId, updateData.get(2));
        printInfo(metaServiceClient);

        // 11. delete
        boolean d1 = executorcApi.delete(tableId, deleteData.get(0));
        printInfo(metaServiceClient);

        // 12. select by in1
        List<Object[]> r31 = executorcApi.select(tableId, selectIn1Data2.get(0), new boolean[]{false, true, true, true, false});
        List<Object[]> r32 = executorcApi.select(tableId, selectIn1Data2.get(1), new boolean[]{false, true, true, true, false});
        List<Object[]> r33 = executorcApi.select(tableId, selectIn1Data2.get(2), new boolean[]{false, true, true, true, false});
        List<Object[]> r34 = executorcApi.select(tableId, selectIn1Data2.get(3), new boolean[]{false, true, true, true, false});

        // 13. select by in2
        List<Object[]> r41 = executorcApi.select(tableId, selectIn2Data2.get(0), new boolean[]{false, true, true, false, true});
        List<Object[]> r42 = executorcApi.select(tableId, selectIn2Data2.get(1), new boolean[]{false, true, true, false, true});
        List<Object[]> r43 = executorcApi.select(tableId, selectIn2Data2.get(2), new boolean[]{false, true, true, false, true});
        List<Object[]> r44 = executorcApi.select(tableId, selectIn2Data2.get(3), new boolean[]{false, true, true, false, true});

        // 14. drop index in1
        tableApi.deleteIndex(tableId, in1.getName());

        // 15. select by in1
        List<Object[]> r51 = executorcApi.select(tableId, selectIn1Data2.get(0), new boolean[]{false, true, true, true, false});
    }

    private static void printInfo(MetaService metaServiceClient) throws Exception {
        CommonId tableId = metaServiceClient.getTableId(tableName);
        IndexExecutor indexExecutor = new IndexExecutor(tableId, (MetaServiceClient) metaServiceClient);

        TableDefinition tableDefinition = metaServiceClient.getTableDefinition(tableName);
        System.out.println("tableDefinition = " + tableDefinition);

        List<Object[]> record1 = indexExecutor.getFinishedRecords();
        System.out.println("FinishedRecord = ");
        record1.stream().forEach(e -> printObjectArray(e));

        List<Object[]> record2 = indexExecutor.getUnfinishRecords();
        System.out.println("UnfinishedRecord = ");
        record2.stream().forEach(e -> printObjectArray(e));

        try {
            List<KeyValue> in1 = indexExecutor.getIndexKeyValue(getIndexIn1().getName());
            System.out.println("in1 = ");
            in1.stream().forEach(e -> printKeyValue(e));
        } catch (Exception e) {
            System.out.println("in1 = " + e.getMessage());
        }

        try {
            List<KeyValue> in2 = indexExecutor.getIndexKeyValue(getIndexIn2().getName());
            System.out.println("in2 = ");
            in2.stream().forEach(e -> printKeyValue(e));
        } catch (Exception e) {
            System.out.println("in2 = " + e.getMessage());
        }
    }

    private static TableDefinition getTableDefinition() {
        ColumnDefinition cd1 = ColumnDefinition.builder().name("cd1")
            .type("VARCHAR")
            .elementType("VARCHAR")
            .precision(-1)
            .scale(Integer.MIN_VALUE)
            .nullable(false)
            .primary(true)
            .defaultValue("0")
            .build();

        ColumnDefinition cd2 = ColumnDefinition.builder().name("cd2")
            .type("FLOAT")
            .elementType("FLOAT")
            .precision(-1)
            .scale(Integer.MIN_VALUE)
            .nullable(false)
            .primary(false)
            .defaultValue("0")
            .build();

        ColumnDefinition cd3 = ColumnDefinition.builder().name("cd3")
            .type("VARCHAR")
            .elementType("VARCHAR")
            .precision(-1)
            .scale(Integer.MIN_VALUE)
            .nullable(false)
            .primary(false)
            .defaultValue("0")
            .build();

        ColumnDefinition cd4 = ColumnDefinition.builder().name("cd4")
            .type("INTEGER")
            .elementType("INTEGER")
            .precision(-1)
            .scale(Integer.MIN_VALUE)
            .nullable(false)
            .primary(false)
            .defaultValue("0")
            .build();

        ColumnDefinition cd5 = ColumnDefinition.builder().name("cd5")
            .type("DOUBLE")
            .elementType("DOUBLE")
            .precision(-1)
            .scale(Integer.MIN_VALUE)
            .nullable(false)
            .primary(false)
            .defaultValue("0")
            .build();

        TableDefinition td = new TableDefinition("Test");
        td.addColumn(cd1);
        td.addColumn(cd2);
        td.addColumn(cd3);
        td.addColumn(cd4);
        td.addColumn(cd5);

        return td;
    }

    private static Index getIndexIn1() {
        return new Index("in1", new String[]{"cd3", "cd4", "cd2"}, true);
    }

    private static Index getIndexIn2() {
        return new Index("in2", new String[]{"cd3", "cd2", "cd5"}, false);
    }

    private static List<Object[]> getData1() {
        List<Object[]> records = new ArrayList<>();
        records.add(new Object[]{"1", 1.0, "1", 1, 1.0});
        records.add(new Object[]{"2", 2.0, "2", 2, 2.0});
        records.add(new Object[]{"3", 3.0, "3", 3, 3.0});
        records.add(new Object[]{"4", 4.0, "4", 4, 4.0});
        records.add(new Object[]{"5", 5.0, "5", 5, 5.0});
        return records;
    }

    private static List<Object[]> getData2() {
        List<Object[]> records = new ArrayList<>();
        records.add(new Object[]{"6", 6.0, "6", 6, 6.0});
        records.add(new Object[]{"7", 7.0, "7", 7, 7.0});
        records.add(new Object[]{"8", 8.0, "8", 8, 8.0});
        records.add(new Object[]{"9", 9.0, "9", 9, 9.0});
        records.add(new Object[]{"10", 10.0, "10", 10, 10.0});
        return records;
    }

    private static List<Object[]> getData3() {
        List<Object[]> records = new ArrayList<>();
        records.add(new Object[]{"11", 11.0, "11", 11, 11.0});
        records.add(new Object[]{"12", 12.0, "12", 12, 12.0});
        records.add(new Object[]{"13", 13.0, "13", 13, 13.0});
        records.add(new Object[]{"14", 14.0, "14", 14, 14.0});
        records.add(new Object[]{"15", 15.0, "15", 15, 15.0});
        return records;
    }

    private static List<Object[]> getSelectByIn1Data1() {
        List<Object[]> records = new ArrayList<>();
        records.add(new Object[]{null, 2.0, "2", 2, null});
        records.add(new Object[]{null, 9.0, "9", 9, null});
        records.add(new Object[]{null, 13.0, "13", 13, null});
        return records;
    }

    private static List<Object[]> getSelectByIn2Data1() {
        List<Object[]> records = new ArrayList<>();
        records.add(new Object[]{null, 2.0, "2", null, 2.0});
        records.add(new Object[]{null, 9.0, "9", null, 9.0});
        records.add(new Object[]{null, 13.0, "13", null, 13.0});
        return records;
    }

    private static List<Object[]> getUpdateData() {
        List<Object[]> records = new ArrayList<>();
        records.add(new Object[]{"2", 20.0, "20", 20, 20.0});
        records.add(new Object[]{"3", 30.0, "30", 30, 30.0});
        records.add(new Object[]{"5", 50.0, "50", 50, 50.0});
        return records;
    }

    private static List<Object[]> getDeleteData() {
        List<Object[]> records = new ArrayList<>();
        records.add(new Object[]{"13", 13.0, "13", 13, 13.0});
        return records;
    }

    private static List<Object[]> getSelectByIn1Data2() {
        List<Object[]> records = new ArrayList<>();
        records.add(new Object[]{null, 2.0, "2", 2, null});
        records.add(new Object[]{null, 20.0, "20", 20, null});
        records.add(new Object[]{null, 9.0, "9", 9, null});
        records.add(new Object[]{null, 13.0, "13", 13, null});
        return records;
    }

    private static List<Object[]> getSelectByIn2Data2() {
        List<Object[]> records = new ArrayList<>();
        records.add(new Object[]{null, 2.0, "2", null, 2.0});
        records.add(new Object[]{null, 20.0, "20", null, 20.0});
        records.add(new Object[]{null, 9.0, "9", null, 9.0});
        records.add(new Object[]{null, 13.0, "13", null, 13.0});
        return records;
    }

    private static void printObjectArray(Object[] objects) {
        for (Object object : objects) {
            System.out.print(object + " ");
        }
        System.out.println();
    }

    private static void printKeyValue(KeyValue e) {
        byte[] key = e.getKey();
        byte[] value = e.getValue();
        for (byte b : key) {
            System.out.print(b + " ");
        }
        System.out.print(" : ");
        for (byte b : value) {
            System.out.print(b + " ");
        }
        System.out.println();
    }
}
