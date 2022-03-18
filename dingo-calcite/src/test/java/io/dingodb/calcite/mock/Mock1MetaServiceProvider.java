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
import com.google.common.collect.ImmutableMap;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.meta.Location;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.MetaServiceProvider;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@AutoService(MetaServiceProvider.class)
public class Mock1MetaServiceProvider implements MetaServiceProvider {
    public static final String SCHEMA_NAME = "MOCK1";

    public static final String TABLE_1_NAME = "TEST1";
    public static final Location LOC_0 = new Location("host1", 26535, "/path1");
    public static final Location LOC_1 = new Location("host2", 26535, "/path2");

    @Override
    public MetaService get() {
        MetaService metaService = Mockito.spy(MetaService.class);
        try {
            when(metaService.getName()).thenReturn(SCHEMA_NAME);
            when(metaService.getTableDefinitions()).thenReturn(ImmutableMap.of(
                TABLE_1_NAME, TableDefinition.readJson(getClass().getResourceAsStream("/table-test1.json"))
            ));
            when(metaService.getTableKey(anyString())).then(args -> {
                String tableName = args.getArgument(0);
                return tableName.getBytes(StandardCharsets.UTF_8);
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        when(metaService.getPartLocations(TABLE_1_NAME)).thenReturn(ImmutableMap.of(
            "0", LOC_0,
            "1", LOC_1
        ));
        when(metaService.currentLocation()).thenReturn(LOC_0);
        return metaService;
    }
}
