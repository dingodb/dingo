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
import io.dingodb.sdk.annotation.DingoRecord;
import io.dingodb.sdk.client.DingoClient;
import io.dingodb.sdk.client.DingoOpCli;
import io.dingodb.sdk.common.Column;
import io.dingodb.sdk.common.Key;
import io.dingodb.sdk.common.Record;
import io.dingodb.sdk.mock.MockApiRegistry;
import io.dingodb.sdk.mock.MockMetaClient;
import io.dingodb.sdk.model.Address;
import io.dingodb.sdk.model.ComplexStruct;
import io.dingodb.sdk.model.ConstructedClass;
import io.dingodb.sdk.model.basic.Person;
import io.dingodb.sdk.operation.StoreOperationUtils;
import io.dingodb.sdk.utils.MetaServiceUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;

public class TestDingoComplexType {

    private DingoClient dingoClient;

    private MockMetaClient metaClient = new MockMetaClient("src/test/resources/config.yaml");

    private MockApiRegistry apiRegistry = new MockApiRegistry();

    private ComplexStruct complexStruct;

    @BeforeEach
    public void init() {
        dingoClient = new DingoClient("src/test/resources/config/config.yaml");
        MetaServiceUtils.initConnectionInMockMode(dingoClient, metaClient, apiRegistry);

        complexStruct = new ComplexStruct();
        complexStruct.setSsn("123456789");
        complexStruct.setValues(Arrays.asList(1, 2,3,4,5));

        complexStruct.setAddresses(Arrays.asList(
            new Address("street1", "city1"),
            new Address("street2", "city2")
        ));

        HashMap<String, String> home = new HashMap<>();
        home.put("street", "123 Main St");
        home.put("city", "New York");
        complexStruct.setHome(home);

        HashMap<String, Address> work = new HashMap<>();
        work.put("street", new Address("street1", "city1"));
        work.put("city", new Address("street2", "city2"));
        complexStruct.setWork(work);

        Integer[] intArray = new Integer[]{1,2,3,4,5};
        complexStruct.setIntegerValues(intArray);
    }

    @AfterEach
    public void tearDown() {
        if (dingoClient != null) {
            dingoClient.closeConnection();
        }
    }

    @Test
    public void testOperationWithComplexTypeCase1() {
        boolean isOK = dingoClient.openConnection();
        Assertions.assertTrue(isOK);

        DingoOpCli dingoCli = new DingoOpCli.Builder(dingoClient).build();
        isOK = dingoCli.createTable(ComplexStruct.class);
        Assertions.assertTrue(isOK);
        Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
        Assertions.assertEquals(1, storeOperations.size());

        isOK = dingoCli.dropTable(ComplexStruct.class);
        Assertions.assertTrue(isOK);
        storeOperations = StoreOperationUtils.getTableDefinitionInCache();
        Assertions.assertEquals(0, storeOperations.size());
    }

    @Test
    public void testOperationWithComplexTypeCase2() {
        boolean isOK = dingoClient.openConnection();
        Assertions.assertTrue(isOK);

        DingoClient spyClient = Mockito.spy(dingoClient);
        DingoOpCli dingoCli = new DingoOpCli.Builder(spyClient).build();
        isOK = dingoCli.createTable(ComplexStruct.class);
        Assertions.assertTrue(isOK);

        Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
        Assertions.assertEquals(1, storeOperations.size());

        DingoRecord record = ComplexStruct.class.getAnnotation(DingoRecord.class);
        String tableName = record.table();
        TableDefinition tableDefinition = storeOperations.get(tableName);
        Assertions.assertTrue(tableDefinition != null);
        Record expectedRecord = new Record(complexStruct, false);

        try {
            doReturn(true).when(spyClient).put(any(), (Column[]) any());
            doReturn(expectedRecord).when(spyClient).get((Key) any());
            doReturn(true).when(spyClient).delete((Key) any());
            doReturn(true).when(spyClient).dropTable(anyString());
        } catch (Exception e) {
            Assertions.fail("Mock catch Unexpected exception");
        }
        dingoCli.save(complexStruct);

        ComplexStruct localStruct = dingoCli.read(ComplexStruct.class, "123456789");
        Assertions.assertEquals(complexStruct.getSsn(), localStruct.getSsn());
        Assertions.assertEquals(
            Arrays.toString(complexStruct.getAddresses().toArray()),
            Arrays.toString(localStruct.getAddresses().toArray())
        );
        Assertions.assertEquals(
            Arrays.toString(complexStruct.getIntegerValues()),
            Arrays.toString(localStruct.getIntegerValues())
        );
        Assertions.assertEquals(
            complexStruct.getHome().size(),
            localStruct.getHome().size()
        );

        isOK = dingoCli.delete(localStruct);
        Assertions.assertTrue(isOK);

        isOK = dingoCli.dropTable(ComplexStruct.class);
        Assertions.assertTrue(isOK);
    }
}
