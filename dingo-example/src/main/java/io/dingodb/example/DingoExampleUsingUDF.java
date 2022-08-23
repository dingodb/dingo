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

import io.dingodb.sdk.client.DingoClient;
import io.dingodb.sdk.common.Record;

import java.util.ArrayList;
import java.util.List;

public class DingoExampleUsingUDF {

    /*
    CREATE TABLE persons1
    (
        id int NOT NULL,
        name varchar(255) NOT NULL,
        address varchar(255),
        age int,
        test double,
        score int multiset,
        PRIMARY KEY (id)
    );

    insert into persons1 values(2, 'a2', 'd2', 25, 10, multiset[1,2,3,4,5]);
     */

    public static void main(String[] args) throws Exception {
        DingoClient client = new DingoClient("127.0.0.1:19181,127.0.0.1:19182,127.0.0.1:19183");
        client.open();

        String luajFunction = "function test1(o) \r\n"
            + "    o[5][2] = 5 \r\n"
            + "    return o \r\n"
            + "end";
        int version = client.registerUDF("PERSONS1", "test", luajFunction);
        System.out.println(version);

        List<Object> key = new ArrayList<>();
        key.add(2);

        Record record = client.getRecordByUDF("PERSONS1", "test", "test1", version, key);
        for (Object column : record.getDingoColumnValuesInOrder()) {
            System.out.println(column);
        }

        client.updateRecordUsingUDF("PERSONS1", "test", "test1", version, key);

        System.out.println(client.unregisterUDF("PERSONS1", "test", version));

        client.close();
    }
}
