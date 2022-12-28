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

import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.TableDefinition;

import java.util.Arrays;
import java.util.List;

public class GetAllData extends Step0Info {


    public static void main(String[] args) throws Exception {
        TableDefinition tableDefinition = metaServiceClient.getTableDefinition(tableName);
        System.out.println("tableDefinition = " + tableDefinition);

        List<Object[]> record1 = dingoIndexDataExecutor.getFinishedRecord(tableName);
        System.out.println("FinishedRecord = ");
        record1.stream().forEach(e -> printObjectArray(e));

        List<Object[]> record2 = dingoIndexDataExecutor.getUnfinishedRecord(tableName);
        System.out.println("UnfinishedRecord = ");
        record2.stream().forEach(e -> printObjectArray(e));

        List<KeyValue> in1 = dingoIndexDataExecutor.getIndexKeyValue(tableName, getIndexIn1().getName());
        System.out.println("in1 = ");
        in1.stream().forEach(e -> System.out.println(e.getKey() + " " + e.getValue()));

        List<KeyValue> in2 = dingoIndexDataExecutor.getIndexKeyValue(tableName, getIndexIn2().getName());
        System.out.println("in2 = ");
        in2.stream().forEach(e -> System.out.println(e.getKey() + " " + e.getValue()));
    }
}
