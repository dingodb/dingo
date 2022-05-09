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

package io.dingodb.example.sdk.client;

import io.dingodb.sdk.client.DmlClient;

import java.util.ArrayList;
import java.util.List;

public class DmlClientExample {
    private static DmlClient dmlClient;

    public static void main(String[] args) throws Exception {
        dmlClient = new DmlClient(io.dingodb.example.sdk.client.DmlClientExample.class
            .getResource("/coordinator.yaml").getPath(), "PERSONS");
        insert();
        insertBatch();
        get();
        delete();
        System.exit(0);
    }

    public static void insert() throws Exception {
        Object[] record1 = new Object[]{1200, "a1200", "d1200"};
        dmlClient.insert(record1);
    }

    public static void insertBatch() throws Exception {
        List<Object[]> records = new ArrayList<Object[]>();
        for (int i = 0; i < 1100; i ++) {
            Object[] record = new Object[]{i, "a" + i, "d" + i};
            records.add(record);
        }
        dmlClient.insert(records);
    }

    public static void get() throws Exception {
        Object[] key = new Object[]{1200};
        Object[] record = dmlClient.get(key);
        for (Object r : record) {
            System.out.println(r);
        }
    }

    public static void delete() throws Exception {
        Object[] key = new Object[]{1200};
        dmlClient.delete(key);
    }
}
