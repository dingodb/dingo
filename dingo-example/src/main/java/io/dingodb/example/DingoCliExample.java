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

import io.dingodb.example.model.Person;
import io.dingodb.sdk.client.DingoClient;
import io.dingodb.sdk.client.DingoOpCli;


public class DingoCliExample {

    public static void main(String[] args) throws Exception {
        int totalCnt = 100;

        if (args.length > 0) {
            totalCnt = Integer.parseInt(args[0]);
        }

        String remoteHost = "172.20.31.10:19181,172.20.31.11:19181,172.20.31.12:19181";
        DingoClient dingoClient = new DingoClient(remoteHost);
        dingoClient.open();

        DingoOpCli dingoOpCli = new DingoOpCli.Builder(dingoClient).build();
        boolean isOK = dingoOpCli.createTable(Person.class);
        System.out.println("Create table Status: " + isOK);

        for (int i = 0; i < totalCnt; i++) {
            Person person = new Person();
            person.setId(i + 1);
            person.setAge(10 + i);
            person.setName("dingo");
            person.setSalary(1000.0 * i);
            dingoOpCli.save(person);
        }

        for (int i = 0; i < totalCnt; i++) {
            Person person = dingoOpCli.read(Person.class, (i + 1));
            System.out.println(">>>>>>>>>>Read=>" + person);
        }

        System.out.println("Will drop table ....You can cancel...");
        wait(120000);

        isOK = dingoOpCli.dropTable(Person.class);
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
