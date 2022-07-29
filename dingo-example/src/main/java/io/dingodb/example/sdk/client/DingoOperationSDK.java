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

import io.dingodb.common.config.DingoConfiguration;
import io.dingodb.common.operation.Column;
import io.dingodb.common.operation.ExecutiveResult;
import io.dingodb.common.operation.Value;
import io.dingodb.common.operation.context.FilterContext;
import io.dingodb.sdk.client.DingoClient;
import io.dingodb.sdk.common.Key;
import io.dingodb.server.client.config.ClientConfiguration;

import java.util.Collections;
import java.util.List;

public class DingoOperationSDK {

    private static DingoClient dingoClient;

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.exit(1);
            return ;
        }

        String coordinatorCfg = "./conf/client.yaml";
        String op = "max";
        String tableName = "test";
        int startKey = 1;
        int endKey = 10;
        String column = "amount";

        if (args.length >= 5) {
            coordinatorCfg = args[0];
            op = args[1];
            tableName = args[2];
            startKey = Integer.parseInt(args[3]);
            column = args[4];

            endKey = Integer.parseInt(args[5]);
        }

        System.out.println("coordinatorCfg: " + coordinatorCfg
            + ", tableName: " + tableName
            + ", column: " + column);

        DingoConfiguration.parse(coordinatorCfg);
        String coordinatorExchangeSvrList = ClientConfiguration.instance().getCoordinatorExchangeSvrList();
        dingoClient = new DingoClient(coordinatorExchangeSvrList);
        boolean isOK = dingoClient.openConnection();
        if (!isOK) {
            System.out.println("Failed to open connection");
            return ;
        }
        Key primary = new Key("default", tableName, Collections.singletonList(Value.get(startKey)));
        Key endPrimaryKey = new Key("default", tableName, Collections.singletonList(Value.get(endKey)));
        switch (op.toUpperCase()) {
            case "ADD":
                List<ExecutiveResult> add = dingoClient.add(primary, endPrimaryKey, new Column(column, 30));
                for (ExecutiveResult record : add) {
                    System.out.println("===== " + record.getRecord() + ", ==== isSuccess: " + record.isSuccess());
                }
                break;
            case "MAX":
                List<ExecutiveResult> max = dingoClient.max(primary, endPrimaryKey, new Column(column));
                for (ExecutiveResult record : max) {
                    System.out.println("===== " + record.getRecord());
                }
                break;
            case "MIN":
                List<ExecutiveResult> min = dingoClient.min(primary, endPrimaryKey, new Column(column));
                for (ExecutiveResult record : min) {
                    System.out.println("===== " + record.getRecord());
                }
                break;
            case "SUM":
                String function =
                    "function test(o) \r\n"
                    + "return o > 35 \r\n"
                    + "end";
                FilterContext context = new FilterContext(function, "test");
                List<ExecutiveResult> sum = dingoClient.sum(primary, endPrimaryKey, context, new Column(column));
                for (ExecutiveResult record : sum) {
                    System.out.println("===== " + record.getRecord());
                }
                break;
            case "COUNT":
                List<ExecutiveResult> count = dingoClient.count(primary, endPrimaryKey, new Column(column));
                for (ExecutiveResult record : count) {
                    System.out.println("===== " + record.getRecord());
                }
                break;
            default:
        }

    }
}
