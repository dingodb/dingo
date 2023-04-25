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
public class SchemaPrivDefinition extends PrivilegeDefinition {
    private CommonId schema;

    private String schemaName;

    Boolean[] privileges;

    public String getKey() {
        return user + "#" + host + "#" + schema;
    }

    @Builder(toBuilder = true)
    SchemaPrivDefinition(String user, String host, CommonId schema, String schemaName) {
        super(user, host);
        this.schema = schema;
        this.schemaName = schemaName;
    }
}
