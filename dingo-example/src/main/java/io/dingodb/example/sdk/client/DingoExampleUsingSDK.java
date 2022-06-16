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

import io.dingodb.sdk.client.DingoClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

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

        dingoClient = new DingoClient(coordinatorCfg, tableName);
        long startTime = System.currentTimeMillis();
        switch (cmd) {
            case "insert": {
                insert();
                break;
            }
            case "insertBatch": {
                insertBatch();
                break;
            }
            case "scan": {
                scanAllRecords();
                break;
            }

            case "getByRange": {
                getByRange(startScanKey, endScanKey);
                break;
            }
            default: {
                insert();
                insertBatch();
                scanAllRecords();
                delete();
                break;
            }
        }
        long endTime = System.currentTimeMillis();
        System.out.println("realInsertCnt:" + totalRealInsertCnt + ",totalTimeCost: " + (endTime - startTime) + "ms");
        return;
    }

    public static void insert() throws Exception {
        for (int i = 0; i < insertTotalCnt; i++) {
            String uuid = UUID.randomUUID().toString();
            Object[] record = new Object[]{i, "k-" + uuid, "v-" + uuid};
            dingoClient.insert(record);
        }
    }

    public static void insertBatch() throws Exception {
        do {
            long startTime = System.currentTimeMillis();
            while (totalRealInsertCnt < insertTotalCnt) {
                List<Object[]> records = new ArrayList<Object[]>();
                for (int i = 0; i < insertBatchCnt; i++) {
                    if (totalRealInsertCnt >= insertTotalCnt) {
                        break;
                    }
                    String uuid = UUID.randomUUID().toString();
                    Object[] record = new Object[]{totalRealInsertCnt, "k-" + uuid, "v-" + uuid};
                    records.add(record);
                    totalRealInsertCnt++;
                }
                boolean isOK = true;
                do {
                    try {
                        isOK = dingoClient.insert(records);
                    } catch (Exception ex) {
                        try {
                            isOK = false;
                            Thread.sleep(4000);
                        } catch (Exception ex1) {
                            ex1.printStackTrace();
                        }
                        dingoClient.refreshTableMeta();
                    }
                } while (!isOK);
                long totalTimeCost = System.currentTimeMillis() - startTime;
                System.out.println("inserted record: " + totalRealInsertCnt
                    + ", TotalCost: " + totalTimeCost + "ms"
                    + ", AvgCost: " + (totalTimeCost * 1.0 / totalRealInsertCnt) + "ms");
            }
        }
        while (true);
    }
    public static void scanAllRecords() throws Exception {
        long loopCnt = 0L;
        long totalTimeCost = 0L;
        long warmCnt = 0L;
        String stringResult = "";
        for (int i = 0; i < insertTotalCnt; i++) {
            stringResult = "";
            long startTime = System.currentTimeMillis();
            Object[] key = new Object[]{i};
            try {
                Object[] record = dingoClient.get(key);
                for (Object r : record) {
                    stringResult += r.toString();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            long endTime = System.currentTimeMillis();
            warmCnt++;
            if (warmCnt < 1000) {
                continue;
            }
            loopCnt++;
            totalTimeCost += (endTime - startTime);
            if (loopCnt % 100 == 0) {
                System.out.println("AvgTimeCost:" + totalTimeCost * 1.0 / loopCnt
                    + ", LoopCnt:" + loopCnt
                    + ", QueryResult:" + stringResult
                );
            }
        }
    }

    public static void getByRange(int start, int end) throws Exception {
        Object[] startKey = new Object[]{start};
        Object[] endKey = new Object[]{end};
        List<Object[]> records = dingoClient.get(startKey, endKey);
        records.forEach(record -> {
            String rowInStr = Arrays.asList(record)
                .stream().map(Object::toString).collect(Collectors.joining(","));
            System.out.println(rowInStr);
        });
    }

    public static void delete() throws Exception {
        for (int i = 0; i < insertTotalCnt; i++) {
            Object[] key = new Object[]{i};
            dingoClient.delete(key);
        }
    }
}
