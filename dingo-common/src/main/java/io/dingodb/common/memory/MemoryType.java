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

package io.dingodb.common.memory;

import org.apache.commons.lang.StringUtils;

public enum MemoryType {

    GLOBAL("global"),
    CACHE("cache"),
    GENERAL_PLANER("genernal_planer"),
    GENERAL_TP("genernal_tp"),
    GENERAL_AP("genernal_ap"),
    SCHEMA("schema"),
    SCHEMA_GENERNAL("schema_genernal"),
    SCHEMA_CACHE("schema_cache"),
    QUERY("query"),
    SUBQUERY("subquery"),
    PLANER("planer"),
    EXECUTOR("executor"),
    TASK("task"),
    OPERATOR("operator"),
    STORED_FUNCTION("stored_function"),
    STORED_PROCEDURE("stored_procedure"),
    PROCEDURE_CURSOR("procedure_cursor"),
    CURSOR_FETCH("cursor_fetch"),
    OTHER("other");

    private String extensionName;

    MemoryType(String extensionName) {
        this.extensionName = extensionName;
    }

    public static MemoryType nameOf(String m) {
        for (MemoryType memoryType : MemoryType.values()) {
            if (StringUtils.equalsIgnoreCase(memoryType.name(), m)) {
                return memoryType;
            }
        }

        return null;
    }

    public String getExtensionName() {
        return extensionName;
    }

}
