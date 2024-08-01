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

package io.dingodb.common.meta;

public enum SchemaState {
    SCHEMA_NONE(0),
    SCHEMA_DELETE_ONLY(1),
    SCHEMA_WRITE_ONLY(2),
    SCHEMA_WRITE_REORG(3),
    SCHEMA_DELETE_REORG(4),
    SCHEMA_PUBLIC(5),
    SCHEMA_GLOBAL_TXN_ONLY(6);

    private final int code;
    SchemaState(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static SchemaState get(int number) {
        switch (number) {
            case 0:
                return SCHEMA_NONE;
            case 1:
                return SCHEMA_DELETE_ONLY;
            case 2:
                return SCHEMA_WRITE_ONLY;
            case 3:
                return SCHEMA_WRITE_REORG;
            case 4:
                return SCHEMA_DELETE_REORG;
            case 5:
                return SCHEMA_PUBLIC;
            default:
                throw new RuntimeException("invalid schemaState number");
        }
    }
}
