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

package io.dingodb.calcite.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.dingodb.calcite.DingoRootSchema;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.MetaServiceProvider;
import io.dingodb.meta.Part;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.TreeMap;

import static org.mockito.Mockito.when;

//@AutoService(MetaServiceProvider.class)
public class MockMetaServiceProvider implements MetaServiceProvider {

    public static final String SCHEMA_NAME = DingoRootSchema.DEFAULT_SCHEMA_NAME;
    public static final String TABLE_NAME = "TEST";
    public static final Location LOC_0 = new Location("host1", 26535);
    public static final Location LOC_1 = new Location("host2", 26535);
    private static boolean inited = false;

    public synchronized static void init() {
        if (inited) {
            return;
        }
        inited = true;
        try {
            MetaService metaService = MetaService.root();
            //metaService.createSubMetaService(SCHEMA_NAME);
            //metaService = metaService.getSubMetaService(SCHEMA_NAME);
            String test = "test";
            String test1 = "test1";
            String tableDate = "table-with-date";
            String tableArray = "table-with-array";
            metaService.createTable(
                test, TableDefinition.readJson(MockMetaServiceProvider.class.getResourceAsStream("/table-test.json"))
            );
            metaService.createTable
                (test1, TableDefinition.readJson(MockMetaServiceProvider.class.getResourceAsStream("/table-test1.json"))
                );
            metaService.createTable(
                tableDate,
                TableDefinition.readJson(MockMetaServiceProvider.class.getResourceAsStream("/table-with-date.json"))
            );
            metaService.createTable(
                tableArray,
                TableDefinition.readJson(MockMetaServiceProvider.class.getResourceAsStream("/table-with-array.json"))
            );
            TreeMap<ComparableByteArray, Part> rangeSegments = new TreeMap<>();
            byte[] key0 = {};
            byte[] keyA = {1, 0, 0, 1, 0, 0, 0, 2};
            rangeSegments.put(new ComparableByteArray(key0), new Part(key0, LOC_0, ImmutableList.of(LOC_0)));
            rangeSegments.put(new ComparableByteArray(keyA), new Part(keyA, LOC_1, ImmutableList.of(LOC_1)));
            ((io.dingodb.meta.local.MetaService) metaService).setParts(metaService.getTableId(test), rangeSegments);
            ((io.dingodb.meta.local.MetaService) metaService).setParts(metaService.getTableId(test1), rangeSegments);
            ((io.dingodb.meta.local.MetaService) metaService).setParts(metaService.getTableId(tableDate),
                rangeSegments);
            ((io.dingodb.meta.local.MetaService) metaService).setParts(metaService.getTableId(tableArray),
                rangeSegments);
            io.dingodb.meta.local.MetaService.setLocation(LOC_0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MetaService root() {
        MetaService metaService = Mockito.spy(MetaService.class);
        try {
            when(metaService.name()).thenReturn(SCHEMA_NAME);
            when(metaService.getTableDefinitions()).thenReturn(ImmutableMap.of(
                TABLE_NAME, TableDefinition.readJson(getClass().getResourceAsStream("/table-test.json")),
                "test1", TableDefinition.readJson(getClass().getResourceAsStream("/table-test1.json")),
                "table-with-date", TableDefinition.readJson(getClass().getResourceAsStream("/table-with-date.json")),
                "table-with-array", TableDefinition.readJson(getClass().getResourceAsStream("/table-with-array.json"))
            ));
            when(metaService.getTableId(TABLE_NAME)).thenReturn(CommonId.prefix((byte) 0, new byte[]{0, 0}));
        } catch (IOException e) {
            e.printStackTrace();
        }
        TreeMap<ComparableByteArray, Part> rangeSegments = new TreeMap<>();
        byte[] key0 = {};
        byte[] keyA = {1, 0, 0, 1, 0, 0, 0, 2};
        rangeSegments.put(new ComparableByteArray(key0), new Part(key0, LOC_0, ImmutableList.of(LOC_0)));
        rangeSegments.put(new ComparableByteArray(keyA), new Part(keyA, LOC_1, ImmutableList.of(LOC_1)));
        CommonId schemaId = CommonId.prefix((byte) 1, new byte[]{0, 0});
        when(metaService.id()).thenReturn(schemaId);
        when(metaService.getSubMetaServices()).thenReturn(Collections.singletonMap(SCHEMA_NAME, metaService));
        when(metaService.getParts(schemaId, TABLE_NAME)).thenReturn(rangeSegments);
        when(metaService.getDistributes(TABLE_NAME)).thenReturn(ImmutableList.of(LOC_0, LOC_1));
        when(metaService.currentLocation()).thenReturn(LOC_0);
        return metaService;
    }
}
