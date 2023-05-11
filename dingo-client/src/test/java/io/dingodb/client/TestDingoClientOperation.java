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

public class TestDingoClientOperation {

    //private MockMetaClient metaClient = new MockMetaClient("src/test/resources/config.yaml");
    //
    //private MockApiRegistry apiRegistry = new MockApiRegistry();
    //
    //private DingoClient dingoClient;
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
    //public void testCreateTableUsingDingoClient() {
    //    boolean isOpened = dingoClient.open();
    //    Assertions.assertTrue(isOpened);
    //
    //    TableDefinition tableDefinition = MetaServiceUtils.getSimpleTableDefinition("test");
    //    boolean isOK = dingoClient.createTable(tableDefinition);
    //    Assertions.assertTrue(isOK);
    //
    //    Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(1, storeOperations.size());
    //}
    //
    //@Test
    //public void testCreateTableWithOutOpenConnection() {
    //    TableDefinition tableDefinition = MetaServiceUtils.getSimpleTableDefinition("test");
    //    boolean isOK = dingoClient.createTable(tableDefinition);
    //    Assertions.assertFalse(isOK);
    //    Assertions.assertFalse(dingoClient.isConnected());
    //}
    //
    //@Test
    //public void testCreateTableWithInvalidTableDefinition() {
    //    boolean isOpened = dingoClient.open();
    //    Assertions.assertTrue(isOpened);
    //
    //    TableDefinition tableDefinition = MetaServiceUtils.getSimpleTableDefinition("");
    //    try {
    //        boolean isOK = dingoClient.createTable(tableDefinition);
    //        Assertions.assertFalse(isOK);
    //    } catch (Exception e) {
    //        Assertions.assertTrue(e instanceof DingoClientException.InvalidTableName);
    //    }
    //    Assertions.assertTrue(tableDefinition.getName().isEmpty());
    //    Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(0, storeOperations.size());
    //}
    //
    //@Test
    //public void testCreateDropTable() {
    //    boolean isOpened = dingoClient.open();
    //    Assertions.assertTrue(isOpened);
    //
    //    String tableName = "test";
    //    TableDefinition tableDefinition = MetaServiceUtils.getSimpleTableDefinition(tableName);
    //    boolean isOK = dingoClient.createTable(tableDefinition);
    //    Assertions.assertTrue(isOK);
    //    Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(1, storeOperations.size());
    //
    //    isOK = dingoClient.dropTable(tableName);
    //    Assertions.assertTrue(isOK);
    //
    //    storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(0, storeOperations.size());
    //}
    //
    //@Test
    //public void testCreateDropTableWithOutOpenConnection() {
    //    String tableName = "test";
    //    TableDefinition tableDefinition = MetaServiceUtils.getSimpleTableDefinition(tableName);
    //    boolean isOK = dingoClient.createTable(tableDefinition);
    //    Assertions.assertFalse(isOK);
    //    Assertions.assertFalse(dingoClient.isConnected());
    //    Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(0, storeOperations.size());
    //
    //    isOK = dingoClient.dropTable(tableName);
    //    Assertions.assertFalse(isOK);
    //    Assertions.assertFalse(dingoClient.isConnected());
    //
    //    storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(0, storeOperations.size());
    //}
}
