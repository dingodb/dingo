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

import io.dingodb.calcite.DingoRootSchema;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.meta.MetaService;
import io.dingodb.meta.local.LocalMetaService;

import java.io.IOException;

public class MockMetaServiceProvider {

    public static final String SCHEMA_NAME = DingoRootSchema.DEFAULT_SCHEMA_NAME;
    public static final String TABLE_NAME = "TEST";
    public static final Location LOC_0 = new Location("host1", 26535);
    public static final Location LOC_1 = new Location("host1", 26535);
    private static boolean inited = false;

    public synchronized static void init() {
        if (inited) {
            return;
        }
        inited = true;
        try {
            MetaService metaService = MetaService.root();
            metaService.createSubMetaService(SCHEMA_NAME);
            metaService = metaService.getSubMetaService(SCHEMA_NAME);
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

            byte[] key0 = {};
            byte[] keyA = {1, 0, 0, 1, 0, 0, 0, 2};
            CommonId testTableId = metaService.getTableId(test);
            CommonId test1TableId = metaService.getTableId(test1);
            CommonId testDateTableId = metaService.getTableId(tableDate);
            CommonId testArrayTableId = metaService.getTableId(tableArray);

            ((LocalMetaService) metaService).addRangeDistributions(testTableId, key0, keyA);
            ((LocalMetaService) metaService).addRangeDistributions(testTableId, keyA, null);

            ((LocalMetaService) metaService).addRangeDistributions(test1TableId, key0, keyA);
            ((LocalMetaService) metaService).addRangeDistributions(test1TableId, keyA, null);


            ((LocalMetaService) metaService).addRangeDistributions(testDateTableId, key0, keyA);
            ((LocalMetaService) metaService).addRangeDistributions(testDateTableId, keyA, null);

            ((LocalMetaService) metaService).addRangeDistributions(testArrayTableId, key0, keyA);
            ((LocalMetaService) metaService).addRangeDistributions(testArrayTableId, keyA, null);

            LocalMetaService.setLocation(LOC_0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
