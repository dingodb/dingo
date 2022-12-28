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

package io.dingodb.index.example;

import io.dingodb.common.table.ColumnDefinition;
import io.dingodb.common.table.Index;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.index.DingoIndexDataExecutor;
import io.dingodb.index.DingoIndexExecutor;
import io.dingodb.server.client.connector.impl.CoordinatorConnector;
import io.dingodb.server.client.meta.service.MetaServiceClient;

import java.util.ArrayList;
import java.util.List;

public class Step0Info {
    public static String tableName = "TEST";
    public static String coordinatorAddress = "localhost:19181,localhost:19182,localhost:19183";

    public static CoordinatorConnector coordinatorConnector =
        CoordinatorConnector.getCoordinatorConnector(coordinatorAddress);

    public static MetaServiceClient metaServiceClient = new MetaServiceClient(coordinatorConnector);

    public static DingoIndexExecutor dingoIndexExecutor = new DingoIndexExecutor(coordinatorConnector);

    public static DingoIndexDataExecutor dingoIndexDataExecutor = new DingoIndexDataExecutor(coordinatorConnector);

    public static TableDefinition getTableDefinition() {
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

    public static Index getIndexIn1() {
        return new Index("in1", new String[]{"cd3", "cd4", "cd2"}, true);
    }

    public static Index getIndexIn2() {
        return new Index("in2", new String[]{"cd3", "cd2", "cd5"}, false);
    }

    public static List<Object[]> getData1() {
        List<Object[]> records = new ArrayList<>();
        records.add(new Object[]{"1", 1.0, "1", 1, 1.0});
        records.add(new Object[]{"2", 2.0, "2", 2, 2.0});
        records.add(new Object[]{"3", 3.0, "3", 3, 3.0});
        records.add(new Object[]{"4", 4.0, "4", 4, 4.0});
        records.add(new Object[]{"5", 5.0, "5", 5, 5.0});
        return records;
    }

    public static List<Object[]> getData2() {
        List<Object[]> records = new ArrayList<>();
        records.add(new Object[]{"6", 6.0, "6", 6, 6.0});
        records.add(new Object[]{"7", 7.0, "7", 7, 7.0});
        records.add(new Object[]{"8", 8.0, "8", 8, 8.0});
        records.add(new Object[]{"9", 9.0, "9", 9, 9.0});
        records.add(new Object[]{"10", 10.0, "10", 10, 10.0});
        return records;
    }

    public static List<Object[]> getData3() {
        List<Object[]> records = new ArrayList<>();
        records.add(new Object[]{"11", 11.0, "11", 11, 11.0});
        records.add(new Object[]{"12", 12.0, "12", 12, 12.0});
        records.add(new Object[]{"13", 13.0, "13", 13, 13.0});
        records.add(new Object[]{"14", 14.0, "14", 14, 14.0});
        records.add(new Object[]{"15", 15.0, "15", 15, 15.0});
        return records;
    }

    public static List<Object[]> getSelectByIn1Data() {
        List<Object[]> records = new ArrayList<>();
        records.add(new Object[]{null, 2.0, "2", 2, null});
        records.add(new Object[]{null, 9.0, "9", 9, null});
        records.add(new Object[]{null, 13.0, "13", 13, null});
        return records;
    }

    public static List<Object[]> getSelectByIn2Data() {
        List<Object[]> records = new ArrayList<>();
        records.add(new Object[]{null, 2.0, "2", null, 2.0});
        records.add(new Object[]{null, 9.0, "9", null, 9.0});
        records.add(new Object[]{null, 13.0, "13", null, 13.0});
        return records;
    }

    public static void printObjectArray(Object[] objects) {
        for (Object object : objects) {
            System.out.print(object + " ");
        }
        System.out.println();
    }
}
