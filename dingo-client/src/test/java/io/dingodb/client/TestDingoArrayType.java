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

import io.dingodb.client.mock.MockMetaClient;
import io.dingodb.client.model.AnnotatedArrayClass;
import io.dingodb.client.model.ChildClass;
import org.junit.jupiter.api.BeforeEach;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class TestDingoArrayType {

    //private DingoClient dingoClient;

    private MockMetaClient metaClient = new MockMetaClient("src/test/resources/config.yaml");

    private AnnotatedArrayClass arrayClass;

    @BeforeEach
    public void init() {
        //dingoClient = new DingoClient("127.0.0.1:19191");


        int index = 0;
        //MetaServiceUtils.initConnectionInMockMode(dingoClient, metaClient, apiRegistry);
        arrayClass = new AnnotatedArrayClass();
        int value = 1;
        arrayClass.setKey(value);

        byte[] bytes = new byte[]{1, 2, 3, 4, 5};
        arrayClass.setBytes(bytes);

        short[] shortArray = new short[]{1,2,3,4,5};
        arrayClass.setShorts(shortArray);

        int [] intArray = new int[]{1,2,3,4,5};
        arrayClass.setInts(intArray);

        long [] longArray = new long[]{1L,2L,3L,4L,5L};
        arrayClass.setLongs(longArray);

        float [] floatArray = new float[]{1.0f,2.0f,3.0f,4.0f,5.0f};
        arrayClass.setFloats(floatArray);

        double[] doubleArray = new double[]{1.0,2.0,3.0,4.0,5.0};
        arrayClass.setDoubles(doubleArray);

        arrayClass.setStrings(Arrays.asList("1","2","3","4","5").toArray(new String[0]));

        List<ChildClass> childClassList = Arrays.asList(
            new ChildClass(1, "1", 1.0f),
            new ChildClass(2, "2", 2.0f),
            new ChildClass(3, "3", 3.0f)
        );
        arrayClass.setChildren(childClassList.toArray(new ChildClass[0]));
        List<HashMap<String, Object>> childClassMapList = childClassList.stream().map(childClass -> {
            HashMap<String, Object> map = new HashMap<>();
            map.put("valueOfInt", childClass.getValueOfInt());
            map.put("valueOfStr", childClass.getValueOfStr());
            map.put("valueOfFloat", childClass.getValueOfFloat());
            return map;
        }).collect(Collectors.toList());

        arrayClass.setListChildren(childClassList.toArray(new ChildClass[0]));
        arrayClass.setMapChildren(childClassList.toArray(new ChildClass[0]));
    }
    //
    //@AfterEach
    //public void tearDown() {
    //    if (dingoClient != null) {
    //        dingoClient.close();
    //    }
    //}
    //
    //@Test
    //public void testDingoArrayType01() {
    //    boolean isOK = dingoClient.open();
    //    Assertions.assertTrue(isOK);
    //
    //    DingoOpCli dingoCli = new DingoOpCli.Builder(dingoClient).build();
    //    isOK = dingoCli.createTable(AnnotatedArrayClass.class);
    //    Assertions.assertTrue(isOK);
    //    Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(1, storeOperations.size());
    //
    //    isOK = dingoCli.dropTable(AnnotatedArrayClass.class);
    //    Assertions.assertTrue(isOK);
    //    storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(0, storeOperations.size());
    //}
    //
    //@Test
    //public void testDingoArrayType02() {
    //    boolean isOK = dingoClient.open();
    //    Assertions.assertTrue(isOK);
    //
    //    DingoClient spyClient = Mockito.spy(dingoClient);
    //    DingoOpCli dingoCli = new DingoOpCli.Builder(spyClient).build();
    //    isOK = dingoCli.createTable(AnnotatedArrayClass.class);
    //    Assertions.assertTrue(isOK);
    //
    //    Map<String, TableDefinition> storeOperations = StoreOperationUtils.getTableDefinitionInCache();
    //    Assertions.assertEquals(1, storeOperations.size());
    //
    //    DingoRecord record = AnnotatedArrayClass.class.getAnnotation(DingoRecord.class);
    //    String tableName = record.table().toUpperCase();
    //    TableDefinition tableDefinition = storeOperations.get(tableName);
    //    Assertions.assertTrue(tableDefinition != null);
    //
    //    Column[] columns = dingoCli.getColumnsSeqInStore(arrayClass);
    //    Record expectedRecord = Record.toDingoRecord(new Record(tableDefinition.getColumns(), columns));
    //
    //    try {
    //        doReturn(true).when(spyClient).put(any(), anyList());
    //        doReturn(expectedRecord).when(spyClient).get((Key) any());
    //        doReturn(true).when(spyClient).delete((Key) any());
    //        doReturn(true).when(spyClient).dropTable(anyString());
    //    } catch (Exception e) {
    //        Assertions.fail("Mock catch Unexpected exception");
    //    }
    //    dingoCli.save(arrayClass);
    //
    //    AnnotatedArrayClass localArray = dingoCli.read(AnnotatedArrayClass.class, new Object[]{1});
    //    Assertions.assertEquals(arrayClass.getKey(), localArray.getKey());
    //    Assertions.assertEquals(
    //        Arrays.toString(arrayClass.getBytes()),
    //        Arrays.toString(localArray.getBytes())
    //    );
    //    Assertions.assertEquals(
    //        Arrays.toString(arrayClass.getShorts()),
    //        Arrays.toString(localArray.getShorts())
    //    );
    //    Assertions.assertEquals(
    //        Arrays.toString(arrayClass.getInts()),
    //        Arrays.toString(localArray.getInts())
    //    );
    //    Assertions.assertEquals(
    //        Arrays.toString(arrayClass.getLongs()),
    //        Arrays.toString(localArray.getLongs())
    //    );
    //    Assertions.assertEquals(
    //        Arrays.toString(arrayClass.getFloats()),
    //        Arrays.toString(localArray.getFloats())
    //    );
    //
    //
    //    isOK = dingoCli.delete(localArray);
    //    Assertions.assertTrue(isOK);
    //
    //    isOK = dingoCli.dropTable(ComplexStruct.class);
    //    Assertions.assertTrue(isOK);
    //}

}
