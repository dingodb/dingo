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

package io.dingodb.verify.service;

import io.dingodb.common.annotation.ApiDeclaration;
import io.dingodb.common.mysql.SchemataDefinition;
import io.dingodb.common.table.TableDefinition;

public interface InformationService {

    @ApiDeclaration
    void saveSchemata(SchemataDefinition schemataDefinition);

    @ApiDeclaration
    void dropSchemata(String schemaName);

    @ApiDeclaration
    void updateSchemata(SchemataDefinition schemataDefinition);

    @ApiDeclaration
    void saveTableMeta(String schemaName, TableDefinition tableDefinition);

    @ApiDeclaration
    void dropTableMeta(String schemaName, String tableName);

    @ApiDeclaration
    void updateTableMeta(String schemaName, TableDefinition tableDefinition);
}
