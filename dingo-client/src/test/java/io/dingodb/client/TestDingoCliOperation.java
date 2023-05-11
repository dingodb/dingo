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

public class TestDingoCliOperation {
    //private Person person = new Person();
    //private DingoClient dingoClient;
    //
    //private MockMetaClient metaClient = new MockMetaClient("src/test/resources/config.yaml");
    //
    //private MockApiRegistry apiRegistry = new MockApiRegistry();
    //
    //@BeforeEach
    //public void init() {
    //    person.setFirstName("John");
    //    person.setLastName("Doe");
    //    person.setSsn("123456789");
    //    person.setAge(17);
    //    person.setBirthday(new Date());
    //    person.setBirthTime(new Time(12, 0, 0));
    //    person.setBirthTimestamp(new Timestamp(System.currentTimeMillis()));
    //    person.setSalary(1000.0);
    //    person.setSalary2(1000.0f);
    //
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
    //
    //@Test
    //public void testBasicCreateTable() {
    //    boolean isOK = dingoClient.open();
    //    Assertions.assertTrue(isOK);
    //
    //    DingoOpCli dingoCli = new DingoOpCli.Builder(dingoClient).build();
    //    isOK = dingoCli.createTable(Person.class);
    //    Assertions.assertTrue(isOK);
    //    Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(1, storeOperations.size());
    //
    //    isOK = dingoCli.dropTable(Person.class);
    //    Assertions.assertTrue(isOK);
    //    storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(0, storeOperations.size());
    //}
    //
    //@Test
    //public void testBasicCreateTableCase1() {
    //    DingoOpCli dingoCli = new DingoOpCli.Builder(dingoClient).build();
    //    boolean isOK = dingoCli.createTable(Person.class);
    //    Assertions.assertFalse(isOK);
    //    Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(0, storeOperations.size());
    //}
    //
    //@SuppressWarnings("unchecked")
    //@Test
    //public void testBasicCreateTableWithoutKey() {
    //    boolean isOK = dingoClient.open();
    //    Assertions.assertTrue(isOK);
    //
    //    DingoOpCli dingoCli = new DingoOpCli.Builder(dingoClient).build();
    //    try {
    //        isOK = dingoCli.createTable(PersonWithOutKey.class);
    //    } catch (Exception ex) {
    //        Assertions.assertTrue(ex instanceof DingoClientException);
    //        Assertions.assertTrue(ex.getMessage().contains("primary key"));
    //    }
    //    Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(0, storeOperations.size());
    //}
    //
    //@Test
    //public void testCreateTableWithOutTableName() {
    //    boolean isOK = dingoClient.open();
    //    Assertions.assertTrue(isOK);
    //
    //    DingoOpCli dingoCli = new DingoOpCli.Builder(dingoClient).build();
    //    try {
    //        isOK = dingoCli.createTable(PersonWithoutTableName.class);
    //    } catch (Exception ex) {
    //        Assertions.assertTrue(ex instanceof DingoClientException);
    //        Assertions.assertTrue(ex.getMessage().contains("TableName not specified"));
    //    }
    //    Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(0, storeOperations.size());
    //}
    //
    //@Test
    //public void testCreateTableWithOutColumnAnnotation() {
    //    boolean isOK = dingoClient.open();
    //    Assertions.assertTrue(isOK);
    //
    //    DingoOpCli dingoCli = new DingoOpCli.Builder(dingoClient).build();
    //    isOK = dingoCli.createTable(PersonWithoutColumnAnnotation.class);
    //    Assertions.assertTrue(isOK);
    //    Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(1, storeOperations.size());
    //
    //    DingoRecord record = PersonWithoutColumnAnnotation.class.getAnnotation(DingoRecord.class);
    //    String tableName = record.table().toUpperCase();
    //    TableDefinition tableDefinition = storeOperations.get(tableName);
    //    Assertions.assertTrue(tableDefinition != null);
    //
    //    int columnCount = tableDefinition.getColumns().size();
    //    int loopCnt = 0;
    //    for (Field thisField : PersonWithoutColumnAnnotation.class.getDeclaredFields()) {
    //        String filedName = thisField.getName();
    //        isOK = tableDefinition.getColumns().stream().anyMatch(column -> column.getName().equals(filedName));
    //        Assertions.assertTrue(isOK);
    //        loopCnt++;
    //    }
    //    Assertions.assertEquals(columnCount, loopCnt);
    //}
    //
    //@Test
    //public void testCreateTableWithColumnAnnotation() {
    //    boolean isOK = dingoClient.open();
    //    Assertions.assertTrue(isOK);
    //
    //    DingoOpCli dingoCli = new DingoOpCli.Builder(dingoClient).build();
    //    isOK = dingoCli.createTable(Person.class);
    //    Assertions.assertTrue(isOK);
    //    Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(1, storeOperations.size());
    //
    //    DingoRecord record = Person.class.getAnnotation(DingoRecord.class);
    //    String tableName = record.table().toUpperCase();
    //    TableDefinition tableDefinition = storeOperations.get(tableName);
    //    Assertions.assertTrue(tableDefinition != null);
    //
    //    int columnCount = tableDefinition.getColumns().size();
    //    int loopCnt = 0;
    //    for (Field thisField : Person.class.getDeclaredFields()) {
    //        String filedName = thisField.getName();
    //        if (thisField.isAnnotationPresent(DingoColumn.class)) {
    //            DingoColumn thisColumn = thisField.getAnnotation(DingoColumn.class);
    //            filedName = thisColumn.name();
    //        }
    //        String finalColumnName = filedName;
    //        isOK = tableDefinition.getColumns().stream().anyMatch(column -> column.getName().equals(finalColumnName));
    //        Assertions.assertTrue(isOK);
    //        loopCnt++;
    //    }
    //    Assertions.assertEquals(columnCount, loopCnt);
    //}
    //
    //@Test
    //public void testSaveBasicObject() {
    //    boolean isOK = dingoClient.open();
    //    Assertions.assertTrue(isOK);
    //
    //    DingoClient spyClient = Mockito.spy(dingoClient);
    //    DingoOpCli dingoCli = new DingoOpCli.Builder(spyClient).build();
    //    isOK = dingoCli.createTable(Person.class);
    //    Assertions.assertTrue(isOK);
    //    Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(1, storeOperations.size());
    //
    //    DingoRecord record = Person.class.getAnnotation(DingoRecord.class);
    //    String tableName = record.table().toUpperCase();
    //    TableDefinition tableDefinition = storeOperations.get(tableName);
    //    Assertions.assertTrue(tableDefinition != null);
    //    Record expectedRecord = Record.toDingoRecord(new Record(person));
    //
    //    try {
    //        doReturn(true).when(spyClient).put(any(), anyList());
    //        doReturn(expectedRecord).when(spyClient).get((Key) any());
    //        doReturn(true).when(spyClient).delete((Key) any());
    //    } catch (Exception e) {
    //        Assertions.fail("Mock catch Unexpected exception");
    //    }
    //    dingoCli.save(person);
    //
    //    Person localPerson = dingoCli.read(Person.class, new Object[]{"123456789"});
    //    Assertions.assertEquals(person.getSsn(), localPerson.getSsn());
    //    Assertions.assertEquals(person.getAge(), localPerson.getAge());
    //    Assertions.assertEquals(person.getBirthTimestamp(), localPerson.getBirthTimestamp());
    //    System.out.println("Person: " + person);
    //    isOK = dingoCli.delete(person);
    //    Assertions.assertTrue(isOK);
    //}
    //
    //@Test
    //public void testReadMultiRecords() {
    //    boolean isOK = dingoClient.open();
    //    Assertions.assertTrue(isOK);
    //
    //    DingoClient spyClient = Mockito.spy(dingoClient);
    //    DingoOpCli dingoCli = new DingoOpCli.Builder(spyClient).build();
    //    isOK = dingoCli.createTable(Person.class);
    //    Assertions.assertTrue(isOK);
    //    Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(1, storeOperations.size());
    //
    //    DingoRecord record = Person.class.getAnnotation(DingoRecord.class);
    //    String tableName = record.table().toUpperCase();
    //    TableDefinition tableDefinition = storeOperations.get(tableName);
    //    Assertions.assertTrue(tableDefinition != null);
    //    String key1 = "123456789";
    //    person.setSsn(key1);
    //    Record expectedRecord01 = Record.toDingoRecord(new Record(person));
    //    String key2 = "234567891";
    //    person.setSsn(key2);
    //    Record expectedRecord02 = Record.toDingoRecord(new Record(person));
    //
    //    try {
    //        doReturn(true).when(spyClient).put(any(), anyList());
    //        doReturn(expectedRecord01)
    //            .doReturn(expectedRecord02)
    //            .when(spyClient).get((Key) any());
    //    } catch (Exception e) {
    //        Assertions.fail("Mock catch Unexpected exception");
    //    }
    //    person.setSsn(key1);
    //    dingoCli.save(person);
    //    person.setSsn(key2);
    //    dingoCli.save(person);
    //
    //    Person localPerson = dingoCli.read(Person.class, new Object[]{key1});
    //    Assertions.assertEquals(key1, localPerson.getSsn());
    //    Assertions.assertEquals(person.getAge(), localPerson.getAge());
    //    Assertions.assertEquals(person.getBirthTimestamp(), localPerson.getBirthTimestamp());
    //
    //    localPerson = dingoCli.read(Person.class, new Object[]{key2});
    //    Assertions.assertEquals(key2, localPerson.getSsn());
    //    Assertions.assertEquals(person.getAge(), localPerson.getAge());
    //    Assertions.assertEquals(person.getBirthTimestamp(), localPerson.getBirthTimestamp());
    //    System.out.println("Person: " + person);
    //}
    //
    //@Test
    //public void testPutBatch2Table() throws Exception {
    //    boolean isOK = dingoClient.open();
    //    Assertions.assertTrue(isOK);
    //
    //    DingoClient spyClient = Mockito.spy(dingoClient);
    //    DingoOpCli dingoCli = new DingoOpCli.Builder(spyClient).build();
    //    isOK = dingoCli.createTable(Person.class);
    //    Assertions.assertTrue(isOK);
    //
    //    List<String> userKeys = Arrays.asList("123456789", "234567891", "345678912", "456789123");
    //    List<Person> users = new ArrayList<>();
    //    for (String key : userKeys) {
    //        Person localPerson = new Person(person);
    //        localPerson.setSsn(key);
    //        users.add(localPerson);
    //    }
    //    try {
    //        doReturn(true).when(spyClient).put(any(), anyList());
    //        // this method is equals to dingoCli.save(users.toArray());
    //        // dingoCli.save(users.get(0), users.get(1), users.get(2), users.get(3));
    //        dingoCli.save(users.toArray());
    //    } catch (Exception e) {
    //        Assertions.fail("Mock catch Unexpected exception");
    //    }
    //}

}
