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

package example;

import example.model.Person;
import io.dingodb.sdk.client.DingoClient;
import io.dingodb.sdk.client.DingoOpCli;

public class MainClass {

    public static void main(String[] args) {
        Person p = new Person();
        p.setFirstName("John");
        p.setLastName("Doe");
        p.setSsn("123456789");
        p.setAge(17);

        String configFile = "/home/work/zetyun/dingo.git/client.yaml";
        try {
            DingoClient client = new DingoClient(configFile);
            boolean isOK = client.openConnection();
            DingoOpCli dingoCli = new DingoOpCli.Builder(client).build();
            dingoCli.save(p);

            Person person = dingoCli.read(Person.class, "123456789");
            System.out.println("Person: " + person);
            dingoCli.delete(person);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
