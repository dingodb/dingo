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

package io.dingodb.client;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

public class TestDingoCliWithConstruct {
    //private DingoClient dingoClient;
    //
    //private MockMetaClient metaClient = new MockMetaClient("src/test/resources/config.yaml");
    //
    //private MockApiRegistry apiRegistry = new MockApiRegistry();
    //
    //@BeforeEach
    //public void init() {
    //    dingoClient = new DingoClient("127.0.0.1:19191");
    //    MetaServiceUtils.initConnectionInMockMode(dingoClient, metaClient, apiRegistry);
    //}
    //
    //@AfterEach
    //public void tearDown() {
    //    if (dingoClient != null) {
    //        dingoClient.close();
    //    }
    //}
    //
    //@Test
    //public void testTableOperationUsingConstruct() {
    //    boolean isOK = dingoClient.open();
    //    Assertions.assertTrue(isOK);
    //
    //    DingoClient spyClient = Mockito.spy(dingoClient);
    //    DingoOpCli dingoCli = new DingoOpCli.Builder(spyClient).build();
    //    isOK = dingoCli.createTable(ConstructedClass.class);
    //    Assertions.assertTrue(isOK);
    //
    //    Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(1, storeOperations.size());
    //    String tableName = ConstructedClass.class.getAnnotation(DingoRecord.class).table().toUpperCase();
    //    Assertions.assertTrue(storeOperations.containsKey(tableName));
    //
    //    TableDefinition tableDefinition = storeOperations.get(tableName);
    //    for (Field thisField: ConstructedClass.class.getDeclaredFields()) {
    //        String columnName = thisField.getName();
    //        Assertions.assertTrue(
    //            tableDefinition.getColumns().stream()
    //                .anyMatch(column -> column.getName().equals(columnName)));
    //    }
    //
    //    isOK = dingoCli.dropTable(ConstructedClass.class);
    //    Assertions.assertTrue(isOK);
    //    storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(0, storeOperations.size());
    //}
    //
    //@Test
    //public void testTableOperation() {
    //    boolean isOK = dingoClient.open();
    //    Assertions.assertTrue(isOK);
    //
    //    DingoClient spyClient = Mockito.spy(dingoClient);
    //    DingoOpCli dingoCli = new DingoOpCli.Builder(spyClient).build();
    //    isOK = dingoCli.createTable(ConstructedClass.class);
    //    Assertions.assertTrue(isOK);
    //
    //    ConstructedClass constructedInstance = new ConstructedClass(1, 10, "John", new Date());
    //    Record expectedRecord = Record.toDingoRecord(new Record(constructedInstance));
    //
    //    try {
    //        doReturn(true).when(spyClient).put(any(), anyList());
    //        doReturn(expectedRecord).when(spyClient).get((Key) any());
    //        doReturn(true).when(spyClient).delete((Key) any());
    //        dingoCli.save(constructedInstance);
    //        ConstructedClass actualObject = dingoCli.read(ConstructedClass.class, new Object[]{constructedInstance.id});
    //        Assertions.assertEquals(constructedInstance.id, actualObject.id);
    //        Assertions.assertEquals(constructedInstance.age, actualObject.age);
    //        Assertions.assertEquals(constructedInstance.name, actualObject.name);
    //        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    //        String expectedBirthDay = dateFormat.format(constructedInstance.birthday);
    //        Assertions.assertEquals(expectedBirthDay, actualObject.birthday.toString());
    //        isOK = dingoCli.delete(constructedInstance);
    //        Assertions.assertTrue(isOK);
    //    } catch (DingoClientException e) {
    //        Assertions.fail(e.getMessage());
    //    } catch (Exception e) {
    //        Assertions.fail(e.getMessage());
    //    }
    //}
    //
    //@Test
    //public void getTestTableOperationWithNull() {
    //    boolean isOK = dingoClient.open();
    //    Assertions.assertTrue(isOK);
    //
    //    DingoClient spyClient = Mockito.spy(dingoClient);
    //    DingoOpCli dingoCli = new DingoOpCli.Builder(spyClient).build();
    //    isOK = dingoCli.createTable(ConstructedClass.class);
    //    Assertions.assertTrue(isOK);
    //
    //    ConstructedClass constructedInstance = new ConstructedClass(1, 10, "John", new Date());
    //    constructedInstance.birthday = null;
    //    Record expectedRecord = Record.toDingoRecord(new Record(constructedInstance));
    //
    //    try {
    //        // doReturn(true).when(spyClient).put(any(), (Column[]) any());
    //        doReturn(true).when(spyClient).put(any(), anyList());
    //        doReturn(expectedRecord).when(spyClient).get((Key) any());
    //        doReturn(true).when(spyClient).delete((Key) any());
    //        dingoCli.save(constructedInstance);
    //        ConstructedClass actualObject = dingoCli.read(ConstructedClass.class, new Object[]{constructedInstance.id});
    //        Assertions.assertEquals(constructedInstance.id, actualObject.id);
    //        Assertions.assertEquals(constructedInstance.age, actualObject.age);
    //        Assertions.assertEquals(constructedInstance.name, actualObject.name);
    //        Assertions.assertEquals(constructedInstance.birthday, null);
    //        isOK = dingoCli.delete(constructedInstance);
    //        Assertions.assertTrue(isOK);
    //    } catch (DingoClientException e) {
    //        Assertions.fail(e.getMessage());
    //    } catch (Exception e) {
    //        Assertions.fail(e.getMessage());
    //    }
    //}
    //
    //@Test
    //public void testTableOperationUsingUpdate01() {
    //    boolean isOK = dingoClient.open();
    //    Assertions.assertTrue(isOK);
    //
    //    DingoClient spyClient = Mockito.spy(dingoClient);
    //    DingoOpCli dingoCli = new DingoOpCli.Builder(spyClient).build();
    //    isOK = dingoCli.createTable(ConstructedClass.class);
    //    Assertions.assertTrue(isOK);
    //
    //    ConstructedClass constructedInstance = new ConstructedClass(1, 10, "John", new Date());
    //    Record expectedRecord = Record.toDingoRecord(new Record(constructedInstance));
    //
    //    try {
    //        doReturn(null).when(spyClient).get((Key) any());
    //        doReturn(true).when(spyClient).put(any(), (Column[]) any());
    //        doReturn(expectedRecord).when(spyClient).get((Key) any());
    //        doReturn(true).when(spyClient).delete((Key) any());
    //        isOK = dingoCli.update(constructedInstance, "name", "birthday");
    //        Assertions.assertTrue(isOK);
    //        ConstructedClass actualObject = dingoCli.read(ConstructedClass.class, new Object[]{constructedInstance.id});
    //        Assertions.assertEquals(constructedInstance.id, actualObject.id);
    //        Assertions.assertEquals(constructedInstance.age, actualObject.age);
    //        Assertions.assertEquals(constructedInstance.name, actualObject.name);
    //        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    //        String expectedBirthDay = dateFormat.format(constructedInstance.birthday);
    //        Assertions.assertEquals(expectedBirthDay, actualObject.birthday.toString());
    //        isOK = dingoCli.delete(constructedInstance);
    //        Assertions.assertTrue(isOK);
    //    } catch (DingoClientException e) {
    //        e.printStackTrace();
    //        Assertions.fail(e.getMessage());
    //    } catch (Exception e) {
    //        Assertions.fail(e.getMessage());
    //    }
    //}
    //
    //@Test
    //public void testTableOperationUsingUpdate02() {
    //    boolean isOK = dingoClient.open();
    //    Assertions.assertTrue(isOK);
    //
    //    DingoClient spyClient = Mockito.spy(dingoClient);
    //    DingoOpCli dingoCli = new DingoOpCli.Builder(spyClient).build();
    //    isOK = dingoCli.createTable(ConstructedClass.class);
    //    Assertions.assertTrue(isOK);
    //
    //    ConstructedClass constructedInstance = new ConstructedClass(1, 10, "Jonny", new Date());
    //    ConstructedClass oldConstructedInstance = new ConstructedClass(1, 10, "xyz", null);
    //    Record oldRecord = Record.toDingoRecord(new Record(oldConstructedInstance));
    //    Record newRecord = Record.toDingoRecord(new Record(constructedInstance));
    //
    //    try {
    //        doReturn(oldRecord).doReturn(newRecord).when(spyClient).get((Key) any());
    //        doReturn(true).when(spyClient).put(any(), (Column[]) any());
    //        doReturn(true).when(spyClient).delete((Key) any());
    //        isOK = dingoCli.update(constructedInstance, "name", "birthday");
    //        Assertions.assertTrue(isOK);
    //        ConstructedClass actualObject = dingoCli.read(ConstructedClass.class, new Object[]{constructedInstance.id});
    //        Assertions.assertEquals(constructedInstance.id, actualObject.id);
    //        Assertions.assertEquals(constructedInstance.name, actualObject.name);
    //        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    //        String expectedBirthDay = dateFormat.format(constructedInstance.birthday);
    //        Assertions.assertEquals(expectedBirthDay, actualObject.birthday.toString());
    //        isOK = dingoCli.delete(constructedInstance);
    //        Assertions.assertTrue(isOK);
    //    } catch (DingoClientException e) {
    //        Assertions.fail(e.getMessage());
    //    } catch (Exception e) {
    //        Assertions.fail(e.getMessage());
    //    }
    //}
    //
    //@Test
    //public void testTableOperationUsingUpdate03() {
    //    boolean isOK = dingoClient.open();
    //    Assertions.assertTrue(isOK);
    //
    //    DingoClient spyClient = Mockito.spy(dingoClient);
    //    DingoOpCli dingoCli = new DingoOpCli.Builder(spyClient).build();
    //    isOK = dingoCli.createTable(ConstructedClass.class);
    //    Assertions.assertTrue(isOK);
    //
    //    ConstructedClass constructedInstance = new ConstructedClass(1, 10, "Jonny", new Date());
    //    ConstructedClass oldConstructedInstance = new ConstructedClass(1, 10, "Huzx", null);
    //    Record oldRecord = Record.toDingoRecord(new Record(oldConstructedInstance));
    //    Record newRecord = Record.toDingoRecord(new Record(constructedInstance));
    //
    //    // using invalid columns
    //    String invalidColumn = "invalidColumn";
    //    try {
    //        doReturn(oldRecord).doReturn(newRecord).when(spyClient).get((Key) any());
    //        doReturn(true).when(spyClient).put(any(), (Column[]) any());
    //        doReturn(true).when(spyClient).delete((Key) any());
    //        isOK = dingoCli.update(constructedInstance, "name", "birthday", invalidColumn);
    //    } catch (DingoClientException e) {
    //        Assertions.assertTrue(e.getMessage().contains(invalidColumn));
    //    } catch (Exception e) {
    //        Assertions.fail(e.getMessage());
    //    }
    //}

}
