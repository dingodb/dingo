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

import io.dingodb.client.model.embeded.OwnedClass;
import io.dingodb.client.model.embeded.OwningClass;
import org.junit.jupiter.api.Test;

import java.util.Date;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;


public class TestDingoEmbeddedType {

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
    //public void testDingoEmbededType01() {
    //    boolean isOK = dingoClient.open();
    //    Assertions.assertTrue(isOK);
    //
    //    DingoOpCli dingoCli = new DingoOpCli.Builder(dingoClient).build();
    //    isOK = dingoCli.createTable(OwningClass.class);
    //    Assertions.assertTrue(isOK);
    //    Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(1, storeOperations.size());
    //
    //    isOK = dingoCli.dropTable(OwningClass.class);
    //    Assertions.assertTrue(isOK);
    //    storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(0, storeOperations.size());
    //}
    //
    //@Test
    //public void testEmbededType02() {
    //    boolean isOK = dingoClient.open();
    //    Assertions.assertTrue(isOK);
    //
    //    DingoClient spyClient = Mockito.spy(dingoClient);
    //    DingoOpCli dingoCli = new DingoOpCli.Builder(spyClient).build();
    //    isOK = dingoCli.createTable(OwningClass.class);
    //    Assertions.assertTrue(isOK);
    //
    //    Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(1, storeOperations.size());
    //
    //    OwningClass owner = new OwningClass();
    //    owner.id = 1;
    //    owner.children.add(new OwnedClass("a", 1, new Date().getTime()));
    //    owner.children.add(new OwnedClass("b", 2, new Date(new Date().getTime() - 5000).getTime()));
    //    owner.children.add(new OwnedClass("c", 3, new Date(new Date().getTime() - 10000).getTime()));
    //
    //    Record expectedRecord = Record.toDingoRecord(new Record(owner));
    //
    //    try {
    //        doReturn(true).when(spyClient).put(any(), anyList());
    //        doReturn(expectedRecord).when(spyClient).get((Key) any());
    //        doReturn(true).when(spyClient).delete((Key) any());
    //        doReturn(true).when(spyClient).dropTable(anyString());
    //    } catch (Exception e) {
    //        Assertions.fail("Mock catch Unexpected exception");
    //    }
    //    dingoCli.save(owner);
    //    /*
    //    OwningClass readObj = dingoCli.read(OwningClass.class, 1);
    //    Assertions.assertEquals(owner.id, readObj.id);
    //    Assertions.assertEquals(owner.children.size(), readObj.children.size());
    //    */
    //}

    @Test
    public void testEmbedType03() {
        OwningClass owner = new OwningClass();
        owner.id = 1;
        owner.children.add(new OwnedClass("a", 1, new Date().getTime()));
        owner.children.add(new OwnedClass("b", 2, new Date(new Date().getTime() - 5000).getTime()));
        owner.children.add(new OwnedClass("c", 3, new Date(new Date().getTime() - 10000).getTime()));
    }
}
