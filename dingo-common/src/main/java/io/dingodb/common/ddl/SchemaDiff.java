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

package io.dingodb.common.ddl;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

@ToString
@Data
public class SchemaDiff {
    long version;
    ActionType type;
    long schemaId;
    long tableId;

    long oldTableId;
    long oldSchemaId;
    String tableName;

    boolean regenerateSchemaMap;

    AffectedOption[] affectedOpts;

    @Builder
    public SchemaDiff(long version,
                      ActionType type,
                      long schemaId,
                      long tableId,
                      long oldTableId,
                      long oldSchemaId,
                      boolean regenerateSchemaMap,
                      AffectedOption[] affectedOpts) {
        this.version = version;
        this.type = type;
        this.schemaId = schemaId;
        this.tableId = tableId;
        this.oldTableId = oldTableId;
        this.oldSchemaId = oldSchemaId;
        this.regenerateSchemaMap = regenerateSchemaMap;
        this.affectedOpts = affectedOpts;
    }

    public SchemaDiff() {

    }

}
