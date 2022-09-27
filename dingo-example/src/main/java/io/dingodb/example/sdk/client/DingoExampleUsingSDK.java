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
import io.dingodb.sdk.client.DingoClient;
import io.dingodb.server.client.config.ClientConfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class DingoExampleUsingSDK {
    private static DingoClient dingoClient;
    private static int  insertBatchCnt = 1000;
    private static int  insertTotalCnt = 20000;

    private static long totalRealInsertCnt = 0L;

    private static int startScanKey = 1;
    private static int endScanKey = 1000;

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: \n\n "
                + "\t\t java -cp dingo-example-all.jar io.dingodb.example.sdk.client.DingoExampleUsingSDK "
                + "<coordinatorCfg> <tableName> <cmd> [<insertBatchCnt>] [<insertTotalCnt>]");
            return ;
        }

        String coordinatorCfg = "./conf/client.yaml";
        String cmd = "insertBatch";
        String tableName = "test";

        if (args.length >= 3 ) {
            coordinatorCfg = args[0];
            tableName = args[1].toUpperCase();
            cmd = args[2];
            insertTotalCnt = args.length > 3 ? Integer.parseInt(args[3]) : insertTotalCnt;
            insertBatchCnt = args.length > 4 ? Integer.parseInt(args[4]) : insertBatchCnt;
            startScanKey = args.length > 5 ? Integer.parseInt(args[5]) : startScanKey;
            endScanKey = args.length > 6 ? Integer.parseInt(args[6]) : endScanKey;
        }

        System.out.println("coordinatorCfg: " + coordinatorCfg
            + ", tableName: " + tableName
            + ", cmd: " + cmd
            + ", insertTotalCnt: " + insertTotalCnt
            + ", insertBatchCnt: " + insertBatchCnt);

        DingoConfiguration.parse(coordinatorCfg);
        String coordinatorServerList = ClientConfiguration.instance().getCoordinatorExchangeSvrList();
        dingoClient = new DingoClient(coordinatorServerList);
        boolean isOK = dingoClient.open();
        if (!isOK) {
            System.out.println("Failed to open connection");
            return;
        }

        long startTime = System.currentTimeMillis();
        switch (cmd) {
            case "insert": {
                insert(tableName);
                break;
            }
            case "insertBatch": {
                insertBatch(tableName);
                break;
            }
            case "scan": {
                scanAllRecords(tableName);
                break;
            }
            case "delete": {
                delete(tableName);
                break;
            }

            default: {
                insert(tableName);
                insertBatch(tableName);
                scanAllRecords(tableName);
                delete(tableName);
                break;
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("realInsertCnt:" + totalRealInsertCnt + ",totalTimeCost: " + (endTime - startTime) + "ms");

        dingoClient.close();
    }

    public static void insert(String tableName) throws Exception {
        for (int i = startScanKey; i < insertTotalCnt; i++) {
            String uuid = UUID.randomUUID().toString();
            Object[] record = new Object[]{i, "k-" + uuid, "v-" + uuid};
            dingoClient.insert(tableName, record);
        }
    }

    public static void insertBatch(String tableName) throws Exception {
        do {
            long startTime = System.currentTimeMillis();
            if (startScanKey != 1) {
                totalRealInsertCnt = startScanKey;
            }

            while (totalRealInsertCnt < insertTotalCnt) {
                List<Object[]> records = new ArrayList<Object[]>();
                for (int i = 0; i < insertBatchCnt; i++) {
                    if (totalRealInsertCnt >= insertTotalCnt) {
                        break;
                    }
                    String uuid = UUID.randomUUID().toString();
                    Object[] record = new Object[] {
                        Long.valueOf(totalRealInsertCnt).intValue(),
                        "k-" + uuid,
                        "v-" + uuid };
                    records.add(record);
                    totalRealInsertCnt++;
                }
                boolean isOK = dingoClient.insert(tableName, records);
                long totalTimeCost = System.currentTimeMillis() - startTime;
                System.out.println("inserted record: " + totalRealInsertCnt
                    + ", TotalCost: " + totalTimeCost + "ms"
                    + ", AvgCost: " + (totalTimeCost * 1.0 / totalRealInsertCnt) + "ms");
            }
            break;
        }
        while (true);
    }

    @SuppressWarnings("unchecked")
    public static void scanAllRecords(String tableName) throws Exception {
        long loopCnt = 0L;
        long totalTimeCost = 0L;
        long warmCnt = 0L;
        String stringResult = "";
        long startTime = 0L;
        for (int i = 0; i < insertTotalCnt; i++) {
            stringResult = "";
            startTime = System.currentTimeMillis();
            Object[] key = new Object[]{i};
            try {
                Object[] record = dingoClient.get(tableName, key);
                for (Object r : record) {
                    stringResult += r.toString();
                    stringResult += ",";
                }
                stringResult.substring(0, stringResult.length() - 1);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            warmCnt++;
            if (warmCnt < 1000) {
                continue;
            }
            long endTime = System.currentTimeMillis();
            totalTimeCost += (endTime - startTime);
            loopCnt++;
            if (loopCnt % 100 == 0) {
                System.out.println("AvgTimeCost:" + totalTimeCost * 1.0 / loopCnt
                    + ", LoopCnt:" + loopCnt
                    + ", QueryResult:" + stringResult
                );
            }
        }
    }

    public static void delete(String tableName) throws Exception {
        for (int i = 0; i < insertTotalCnt; i++) {
            Object[] key = new Object[]{i};
            System.out.println("delete key: " + Arrays.toString(key));
            dingoClient.delete(tableName, key);
        }
    }
}
