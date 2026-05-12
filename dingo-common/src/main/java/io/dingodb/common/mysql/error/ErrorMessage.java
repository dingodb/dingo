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
        errorMap.put(ErrDBDropExists, "Can not drop database '%s'; database doesn't exist");
        errorMap.put(ErrDBCreateExists, "Can not create database '%s'; database exists");
        errorMap.put(ErrDupFieldName, "Duplicate column name '%s'");
        errorMap.put(ErrInvalidDDLState, "Invalid %s state: %s");
        errorMap.put(ErrNoSuchTable, "Table '%s' do not exist");
        errorMap.put(ErrCancelledDDLJob, "Cancelled DDL job");
        errorMap.put(ErrCantDropFieldOrKey, "Can not DROP '%s'; check that column/key exists");
        errorMap.put(ErrTooManyFields, "Too many columns");
        errorMap.put(ErrCantCreateFile, "Can not create file '%s'");
        errorMap.put(ErrTableMustHaveColumns, "A table must have at least 1 column");
        errorMap.put(ErrUnsupportedModifyVec, "Can not modify vector/document index column");
        errorMap.put(ErrModifyColumnNotTran, "modify column, the engine must be transactional");
        errorMap.put(ErrNotFoundDropTable, "Can't find dropped/truncated table '%s'");
        errorMap.put(ErrNotFoundDropSchema, "Can't find dropped schema '%s'");
        errorMap.put(ErrTruncatedWrongValue, "Incorrect %s value: '%s'");
        errorMap.put(ErrKeyDoesNotExist, "Key '%s' doesn't exist in table '%s'");
        errorMap.put(ErrPartitionMgmtOnNonpartitioned, "Partition management on a not partitioned "
            + "table is not possible");
        errorMap.put(ErrDropPartitionNonExistent, "Error in list of partitions to DROP");
        errorMap.put(ErrDupKeyName, "Duplicate key name '%s'");
        errorMap.put(ErrSetDiffTime, "can not set different time in the as of");
        errorMap.put(ErrNoDB, "No database selected");
        errorMap.put(ErrNotSupportedYet, "Not supported yet");
        errorMap.put(ErrRangeNotIncreasing, "VALUES LESS THAN value must be strictly increasing for each partition");
        errorMap.put(ErrRecursiveCteErr,
         "Recursive query aborted after %d iterations. Try increasing @@cte_max_recursion_depth to a larger value");
        errorMap.put(ErrUnknown, "Unknown error");
        errorMap.put(ErrIncorrectGlobalLocalVar, "Variable '%s' is a read-only variable");
        errorMap.put(ErrDBaccessDenied, "Access denied for user '%s'@'%s' to database '%s'");
        errorMap.put(ErrAccessDenied, "Access denied for user '%s'@'%s' (using password: %s)");
        errorMap.put(ErrMustChangePassword, "You must reset your password using ALTER USER statement before "
            + "executing this statement.");
        errorMap.put(ErrFieldNotInGroupBy, "Expression #%d of %s is not in GROUP BY clause and contains "
            + "nonaggregated column '%s' which is not functionally dependent on columns in GROUP BY clause; "
            + "this is incompatible with sql_mode=only_full_group_by");
        errorMap.put(ErrMixOfGroupFuncAndFields, "In aggregated query without GROUP BY, expression #%d of "
            + "SELECT list contains nonaggregated column '%s'; this is incompatible with sql_mode=only_full_group_by");
        errorMap.put(ErrWarnDataOutOfRange, "Out of range value for column '%s' at row %s");
        errorMap.put(ErrKeyColumnDoesNotExits, "Key column '%s' do not exist in table");
        errorMap.put(ErrMultiplePriKey, "Multiple primary key defined");
        errorMap.put(ErrInvalidUseOfNull, "Invalid use of NULL value");
        errorMap.put(ErrDupEntry, "Duplicate entry '%s' for key '%s.PRIMARY'");
        errorMap.put(ErrMultiplePartPriKey, "Multi-partition tables without a primary key do not support adding a primary key");
        errorMap.put(ErrOutOfMemory, "Memory not enough");

    }

    private ErrorMessage() {
    }
}
