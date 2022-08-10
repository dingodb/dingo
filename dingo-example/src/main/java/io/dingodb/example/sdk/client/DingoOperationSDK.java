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
import io.dingodb.common.operation.DingoExecResult;
import io.dingodb.common.operation.Operation;
import io.dingodb.common.operation.Value;
import io.dingodb.common.operation.filter.DingoDateRangeFilter;
import io.dingodb.common.operation.filter.DingoFilter;
import io.dingodb.common.operation.filter.DingoFilterImpl;
import io.dingodb.common.operation.filter.DingoValueEqualsFilter;
import io.dingodb.sdk.client.DingoClient;
import io.dingodb.sdk.common.Key;
import io.dingodb.server.client.config.ClientConfiguration;

import java.util.Arrays;
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
        String c1 = "amount";
        String c2 = "amount2";

        if (args.length >= 5) {
            coordinatorCfg = args[0];
            op = args[1];
            tableName = args[2];
            startKey = Integer.parseInt(args[3]);
            c1 = args[4];

            endKey = Integer.parseInt(args[5]);
            c2 = args[6];
        }

        System.out.println("coordinatorCfg: " + coordinatorCfg
            + ", tableName: " + tableName
            + ", column: " + c1);

        DingoConfiguration.parse(coordinatorCfg);
        String coordinatorExchangeSvrList = ClientConfiguration.instance().getCoordinatorExchangeSvrList();
        dingoClient = new DingoClient(coordinatorExchangeSvrList);
        boolean isOK = dingoClient.open();
        if (!isOK) {
            System.out.println("Failed to open connection");
            return ;
        }
        Key primary = new Key("default", tableName, Collections.singletonList(Value.get(startKey)));
        Key endPrimaryKey = new Key("default", tableName, Collections.singletonList(Value.get(endKey)));

        DingoFilter root = new DingoFilterImpl();
        switch (op.toUpperCase()) {
            case "ADD":
                boolean add = dingoClient.add(primary, endPrimaryKey, new Column(c1, 30));
                System.out.println("===== " + add);

                break;
            case "MAX":
                DingoFilter eqFilter = new DingoValueEqualsFilter(new int[]{1}, new Object[]{43});
                root.addAndFilter(eqFilter);
                List<DingoExecResult> max =
                    dingoClient.max(primary, endPrimaryKey, root, new Column(c1), new Column(c2));
                for (DingoExecResult record : max) {
                    System.out.println("===== " + record.getRecord());
                }
                break;
            case "MIN":
                List<DingoExecResult> min = dingoClient.min(primary, endPrimaryKey, new Column(c1));
                for (DingoExecResult record : min) {
                    System.out.println("===== " + record.getRecord());
                }
                break;
            case "SUM":
                List<DingoExecResult> sum = dingoClient.sum(primary, endPrimaryKey, new Column(c1), new Column(c2));
                for (DingoExecResult record : sum) {
                    System.out.println("===== " + record.getRecord());
                }
                break;
            case "COUNT":
                DingoFilter equalsFilter = new DingoValueEqualsFilter(new int[]{1}, new Object[]{43});
                root.addAndFilter(equalsFilter);
                List<DingoExecResult> count =
                    dingoClient.count(primary, endPrimaryKey, root, new Column(c1), new Column(c2));
                for (DingoExecResult record : count) {
                    System.out.println("===== op: " + record.op() + ", ===== record: " + record.getRecord());
                }
                break;
            case "OPERATE":
                Operation maxOp = Operation.max(new Column("amount"));
                Operation minOp = Operation.min(new Column("amount"));
                List<DingoExecResult> records =
                    dingoClient.operate(primary, endPrimaryKey, Arrays.asList(maxOp, minOp));
                for (DingoExecResult record : records) {
                    System.out.println("===== op: " + record.op() + ", ===== record: " + record.getRecord());
                }
                break;
            case "TRADE":
                DingoFilter dateFilter = new DingoDateRangeFilter(0, 1656604800000L, 1659628800000L);
                DingoFilter tradeEqualsFilter = new DingoValueEqualsFilter(new int[]{5}, new Object[]{333});
                root.addAndFilter(dateFilter);
                root.addAndFilter(tradeEqualsFilter);
                Key startDateKey = new Key("dingo", "trade", Arrays.asList(Value.get("A"), Value.get(1651334400000L)));
                Key endDateKey = new Key("dingo", "trade", Arrays.asList(Value.get("A"), Value.get(1661875200000L)));
                List<DingoExecResult> tradeSum =
                    dingoClient.sum(startDateKey, endDateKey, root, new Column("moveAmount"));
                for (DingoExecResult record : tradeSum) {
                    System.out.println("===== op: " + record.op() + ", ===== record: " + record.getRecord());
                }
                break;
            default:
        }
        dingoClient.close();
    }
}
