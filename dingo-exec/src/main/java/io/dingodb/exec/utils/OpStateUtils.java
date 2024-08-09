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

package io.dingodb.exec.utils;

import io.dingodb.common.meta.SchemaState;

public final class OpStateUtils {

    private OpStateUtils() {
    }

    public static boolean allowWrite(SchemaState schemaState) {
        return schemaState != SchemaState.SCHEMA_DELETE_ONLY
            && schemaState != SchemaState.SCHEMA_NONE;
    }

    public static boolean allowDeleteOnly(SchemaState schemaState) {
        return schemaState != SchemaState.SCHEMA_NONE;
    }

    public static boolean allowOpContinue(String op, SchemaState schemaState) {
        if ("insert".equalsIgnoreCase(op) || "update".equalsIgnoreCase(op)) {
            return allowWrite(schemaState);
        } else if ("delete".equalsIgnoreCase(op)) {
            return allowDeleteOnly(schemaState);
        }
        return true;
    }
}
