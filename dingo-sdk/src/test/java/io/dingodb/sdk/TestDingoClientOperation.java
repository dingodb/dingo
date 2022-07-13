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

package io.dingodb.sdk;

import io.dingodb.common.table.TableDefinition;
import io.dingodb.sdk.client.DingoClient;
import io.dingodb.sdk.client.DingoConnection;
import io.dingodb.sdk.mock.MockApiRegistry;
import io.dingodb.sdk.mock.MockMetaClient;
import io.dingodb.sdk.operation.StoreOperationUtils;
import io.dingodb.sdk.utils.MetaServiceUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDingoClientOperation {

    private MockMetaClient metaClient = new MockMetaClient("src/test/resources/config.yaml");

    private MockApiRegistry apiRegistry = new MockApiRegistry();

    private DingoClient dingoClient;

    @BeforeEach
    public void init() {
        dingoClient = new DingoClient("src/test/resources/config/config.yaml");
        initConnectionInMockMode();
    }

    @AfterEach
    public void tearDown() {
        if (dingoClient != null) {
            dingoClient.closeConnection();
        }
    }

    private void initConnectionInMockMode() {
        DingoConnection connection = mock(DingoConnection.class);
        when(connection.getMetaClient()).thenReturn(metaClient);
        when(connection.getApiRegistry()).thenReturn(apiRegistry);

        try {
            Field metaClientField = DingoConnection.class.getDeclaredField("metaClient");
            metaClientField.setAccessible(true);
            metaClientField.set(connection, metaClient);

            Field apiRegistryField = DingoConnection.class.getDeclaredField("apiRegistry");
            apiRegistryField.setAccessible(true);
            apiRegistryField.set(connection, apiRegistry);

            Field connectionField = DingoClient.class.getDeclaredField("connection");
            connectionField.setAccessible(true);
            connectionField.set(dingoClient, connection);
        } catch (NoSuchFieldException e) {
            Assertions.fail("DingoConnection.metaClient field not found");
        } catch (SecurityException e) {
            Assertions.fail("DingoConnection.metaClient field not accessible");
        } catch (IllegalAccessException e) {
            Assertions.fail("Invalid Runtime Exception");
        }
    }

    @Test
    public void testCreateTableUsingDingoClient() {
        boolean isOpened = dingoClient.openConnection();
        Assertions.assertTrue(isOpened);

        TableDefinition tableDefinition = MetaServiceUtils.getSimpleTableDefinition("test");
        boolean isOK = dingoClient.createTable(tableDefinition);
        Assertions.assertTrue(isOK);

        Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
        Assertions.assertEquals(1, storeOperations.size());
    }

    @Test
    public void testCreateTableWithOutOpenConnection() {
        TableDefinition tableDefinition = MetaServiceUtils.getSimpleTableDefinition("test");
        boolean isOK = dingoClient.createTable(tableDefinition);
        Assertions.assertFalse(isOK);
        Assertions.assertFalse(dingoClient.isConnected());
    }

    @Test
    public void testCreateTableWithInvalidTableDefinition() {
        boolean isOpened = dingoClient.openConnection();
        Assertions.assertTrue(isOpened);

        TableDefinition tableDefinition = MetaServiceUtils.getSimpleTableDefinition("");
        boolean isOK = dingoClient.createTable(tableDefinition);
        Assertions.assertFalse(isOK);
        Assertions.assertTrue(tableDefinition.getName().isEmpty());
        Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
        Assertions.assertEquals(0, storeOperations.size());
    }

    @Test
    public void testCreateDropTable() {
        boolean isOpened = dingoClient.openConnection();
        Assertions.assertTrue(isOpened);

        String tableName = "test";
        TableDefinition tableDefinition = MetaServiceUtils.getSimpleTableDefinition(tableName);
        boolean isOK = dingoClient.createTable(tableDefinition);
        Assertions.assertTrue(isOK);
        Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
        Assertions.assertEquals(1, storeOperations.size());

        isOK = dingoClient.dropTable(tableName);
        Assertions.assertTrue(isOK);

        storeOperations = StoreOperationUtils.getTableDefinitionInCache();
        Assertions.assertEquals(0, storeOperations.size());
    }

    @Test
    public void testCreateDropTableWithOutOpenConnection() {
        String tableName = "test";
        TableDefinition tableDefinition = MetaServiceUtils.getSimpleTableDefinition(tableName);
        boolean isOK = dingoClient.createTable(tableDefinition);
        Assertions.assertFalse(isOK);
        Assertions.assertFalse(dingoClient.isConnected());
        Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
        Assertions.assertEquals(0, storeOperations.size());

        isOK = dingoClient.dropTable(tableName);
        Assertions.assertFalse(isOK);
        Assertions.assertFalse(dingoClient.isConnected());

        storeOperations = StoreOperationUtils.getTableDefinitionInCache();
        Assertions.assertEquals(0, storeOperations.size());
    }
}
