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

import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.common.util.ByteArrayUtils.ComparableByteArray;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.MetaServiceProvider;
import io.dingodb.meta.Part;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.TreeMap;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@AutoService(MetaServiceProvider.class)
public class MockMetaServiceProvider implements MetaServiceProvider {
    public static final String SCHEMA_NAME = "MOCK".toLowerCase();
    public static final String TABLE_NAME = "TEST".toLowerCase();
    public static final String TABLE_1_NAME = "TEST1".toLowerCase();
    public static final Location LOC_0 = new Location("host1", 26535);
    public static final Location LOC_1 = new Location("host2", 26535);

    @Override
    public MetaService get() {
        MetaService metaService = Mockito.spy(MetaService.class);
        try {
            when(metaService.getName()).thenReturn(SCHEMA_NAME);
            when(metaService.getTableDefinitions()).thenReturn(ImmutableMap.of(
                TABLE_NAME, TableDefinition.readJson(getClass().getResourceAsStream("/table-test.json")),
                TABLE_1_NAME, TableDefinition.readJson(getClass().getResourceAsStream("/table-test1.json")),
                "table-with-date", TableDefinition.readJson(getClass().getResourceAsStream("/table-with-date.json"))
            ));
            when(metaService.getTableId(anyString())).thenReturn(CommonId.prefix((byte) 0));
        } catch (IOException e) {
            e.printStackTrace();
        }
        TreeMap<ComparableByteArray, Part> rangeSegments = new TreeMap<>();
        byte[] key0 = {};
        byte[] keyA = {4};
        rangeSegments.put(new ComparableByteArray(key0), new Part(key0, LOC_0, ImmutableList.of(LOC_0)));
        rangeSegments.put(new ComparableByteArray(keyA), new Part(keyA, LOC_1, ImmutableList.of(LOC_1)));
        when(metaService.getParts(TABLE_NAME)).thenReturn(rangeSegments);
        when(metaService.getDistributes(TABLE_NAME)).thenReturn(ImmutableList.of(LOC_0, LOC_1));
        when(metaService.currentLocation()).thenReturn(LOC_0);
        return metaService;
    }
}
