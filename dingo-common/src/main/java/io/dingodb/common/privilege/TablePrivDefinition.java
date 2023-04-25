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

package io.dingodb.common.privilege;

import io.dingodb.common.CommonId;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@NoArgsConstructor
@ToString
public class TablePrivDefinition extends PrivilegeDefinition {
    private CommonId schema;

    private String schemaName;
    private CommonId table;

    private String tableName;

    Boolean[] privileges;

    public String getKey() {
        StringBuilder schemaPrivKey = new StringBuilder();
        return schemaPrivKey.append(user)
            .append("#").append(host)
            .append("#").append(schema)
            .append("#").append(table).toString();
    }

    @Builder(toBuilder = true)
    TablePrivDefinition(String user, String host, CommonId schema, CommonId table,
                        String schemaName, String tableName) {
        super(user, host);
        this.schema = schema;
        this.table = table;
        this.schemaName = schemaName;
        this.tableName = tableName;
    }
}
