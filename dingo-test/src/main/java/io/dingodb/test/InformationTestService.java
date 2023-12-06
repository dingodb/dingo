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

package io.dingodb.test;

import io.dingodb.common.mysql.SchemataDefinition;
import io.dingodb.common.table.TableDefinition;
import io.dingodb.verify.service.InformationService;

public class InformationTestService implements InformationService {
    public static final InformationTestService INSTANCE = new InformationTestService();

    public InformationTestService() {
        System.out.println(":");
    }

    @Override
    public void saveSchemata(SchemataDefinition schemataDefinition) {

    }

    @Override
    public void dropSchemata(String schemaName) {

    }

    @Override
    public void updateSchemata(SchemataDefinition schemataDefinition) {

    }

    @Override
    public void saveTableMeta(String schemaName, TableDefinition tableDefinition) {

    }

    @Override
    public void dropTableMeta(String schemaName, String tableName) {

    }

    @Override
    public void updateTableMeta(String schemaName, TableDefinition tableDefinition) {

    }

}
