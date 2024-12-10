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

package io.dingodb.common.mysql.error;

import java.util.HashMap;
import java.util.Map;

import static io.dingodb.common.mysql.error.ErrorCode.*;

public final class ErrorMessage {
    public static Map<Integer, String> errorMap = new HashMap<>();

    static {
        errorMap.put(WarnDataTruncated, "Data truncated for column '%s' at row %d");
        errorMap.put(ErrDBDropExists, "Can't drop database '%s'; database doesn't exist");
        errorMap.put(ErrDBCreateExists, "Can't create database '%s'; database exists");
        errorMap.put(ErrDupFieldName, "Duplicate column name '%s'");
        errorMap.put(ErrInvalidDDLState, "Invalid %s state: %s");
        errorMap.put(ErrNoSuchTable, "Table '%s' doesn't exist");
        errorMap.put(ErrCancelledDDLJob, "Cancelled DDL job");
        errorMap.put(ErrCantDropFieldOrKey, "Can't DROP '%s'; check that column/key exists");
    }

    private ErrorMessage() {
    }
}
