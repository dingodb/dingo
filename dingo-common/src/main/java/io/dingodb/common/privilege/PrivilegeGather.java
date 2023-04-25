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
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;

@Setter
@Getter
@Builder
@ToString
public class PrivilegeGather implements Serializable {
    private static final long serialVersionUID = 3710791330563076956L;

    private String user;
    private String host;
    private UserDefinition userDef;
    private Map<String, SchemaPrivDefinition> schemaPrivDefMap;
    private Map<String, TablePrivDefinition> tablePrivDefMap;

    public String key() {
        return user + "#" + host;
    }

}
