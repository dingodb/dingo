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

public class DingoExampleSDKVerify {

    private static DingoClient dingoClient;

    private static int  selectTotalCnt = 2;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Usage: \n\n "
                + "\t\t java -cp dingo-example-all.jar io.dingodb.example.sdk.client.DingoExampleSDKVerify "
                + "<coordinatorCfg> <tableName> <user> <password>");
            return ;
        }

        String coordinatorCfg = args[0];
        System.out.println("coordinatorCfg: " + coordinatorCfg);

        DingoConfiguration.parse(coordinatorCfg);
        String coordinatorServerList = ClientConfiguration.instance().getCoordinatorExchangeSvrList();
        dingoClient = new DingoClient(coordinatorServerList);
        dingoClient.setIdentity(args[2], args[3]);

        boolean isOK = dingoClient.open();
        if (isOK) {
            scanAllRecords(args[1]);
        }
    }

    @SuppressWarnings("unchecked")
    public static void scanAllRecords(String tableName) throws Exception {
        long loopCnt = 0L;
        long totalTimeCost = 0L;
        String stringResult = "";
        long startTime = 0L;
        for (int i = 0; i < selectTotalCnt; i++) {
            stringResult = "";
            startTime = System.currentTimeMillis();
            Object[] key = new Object[]{i};
            try {
                Object[] record = dingoClient.getWithVerify(tableName, key);
                System.out.println(" record size:" + record.length);
                for (Object r : record) {
                    System.out.println(" record item:" + r);
                    stringResult += r.toString();
                    stringResult += ",";
                }
                stringResult.substring(0, stringResult.length() - 1);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
            long endTime = System.currentTimeMillis();
            totalTimeCost += (endTime - startTime);

            System.out.println("AvgTimeCost:" + totalTimeCost * 1.0 / loopCnt
                + ", LoopCnt:" + loopCnt
                + ", QueryResult:" + stringResult
            );

        }
    }
}
