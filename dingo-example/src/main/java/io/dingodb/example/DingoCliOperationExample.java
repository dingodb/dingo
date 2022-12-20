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

import io.dingodb.example.model.PojoModel;
import io.dingodb.sdk.client.DingoClient;
import io.dingodb.sdk.client.DingoOpCli;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;


public class DingoCliOperationExample {

    public static void main(String[] args) {
        int totalCnt = 10;

        String remoteHost = "172.20.31.10:19181,172.20.31.11:19181,172.20.31.12:19181";
        DingoClient dingoClient = new DingoClient(remoteHost);
        dingoClient.setIdentity("root", "123123");
        dingoClient.open();

        DingoOpCli dingoOpCli = new DingoOpCli.Builder(dingoClient).build();
        boolean isOK = dingoOpCli.createTable(PojoModel.class);
        System.out.println("Create table Status: " + isOK);

        for (int i = 0; i < totalCnt; i++) {
            PojoModel pojoModel = new PojoModel();
            pojoModel.setId(i + 1);
            pojoModel.setName("Name-" + i);
            pojoModel.setSalary(1.0 * i);
            pojoModel.setMan(i % 2 == 0);

            LocalDate localDate = LocalDate.of(2020, 1, 1);
            pojoModel.setBirthday(Date.valueOf(localDate));

            LocalTime localTime = LocalTime.of(10, 10, 10);
            pojoModel.setBirthTime(Time.valueOf(localTime));

            LocalDateTime localDateTime = LocalDateTime.of(2020, 1, 1, 10, 10, 10);
            pojoModel.setBirthtimestamp(Timestamp.valueOf(localDateTime));

            List<String> addressList = Arrays.asList("address-" + String.valueOf(i), "beijing");
            pojoModel.setAddress(addressList);

            Set<String> locationSet = new java.util.HashSet<>(addressList);
            pojoModel.setLocation(locationSet);

            HashMap<Integer, String> interest = new HashMap<>();
            interest.put(i, "beijing");
            interest.put(i + 1, "shanghai");
            pojoModel.setInterest(interest);
            dingoOpCli.save(pojoModel);
        }

        for (int i = 0; i < totalCnt; i++) {
            PojoModel pojoModel = dingoOpCli.read(PojoModel.class, new Object[]{i + 1});
            System.out.println(pojoModel);
        }

        System.out.println("Will drop table ....You can cancel...");
        wait(120000);

        isOK = dingoOpCli.dropTable(PojoModel.class);
        System.out.println("drop table Status:" + isOK + ".............");

        dingoClient.close();
    }

    private static void wait(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }
}
