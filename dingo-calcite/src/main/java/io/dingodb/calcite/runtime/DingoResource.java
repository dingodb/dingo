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

package io.dingodb.calcite.runtime;

import io.dingodb.common.exception.DingoSqlException;
import org.apache.calcite.runtime.Resources;

import static org.apache.calcite.runtime.Resources.BaseMessage;
import static org.apache.calcite.runtime.Resources.ExInst;

public interface DingoResource {
    DingoResource DINGO_RESOURCE = Resources.create(DingoResource.class);

    @BaseMessage("Primary keys are required in table ''{0}''")
    ExInst<DingoSqlException> primaryKeyRequired(String a0);

    @BaseMessage("Specified primary keys ''{0}'' does not exist in table ''{1}''")
    ExInst<DingoSqlException> primaryKeyNotExist(String pkName, String tableName);

    @BaseMessage("''{0}'' command denied to user ''{1}''@''{2}'' for table ''{3}''")
    ExInst<DingoSqlException> operatorDenied(String a0, String a1, String a2, String a3);

    @BaseMessage("Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation")
    ExInst<DingoSqlException> createUserDenied();

    @BaseMessage("Invalid default value for ''{0}''")
    ExInst<DingoSqlException> invalidDefaultValue(String a0);

    @BaseMessage("Invalid schema value for ''{0}''")
    ExInst<DingoSqlException> invalidSchemaName(String a0);

    @BaseMessage("Invalid table value for ''{0}''")
    ExInst<DingoSqlException> invalidTableName(String a0);

    @BaseMessage("Invalid column value for ''{0}''")
    ExInst<DingoSqlException> invalidColumnName(String a0);

    @BaseMessage("Incorrect column specifier for column ''{0}''")
    ExInst<DingoSqlException> specifierForColumn(String a0);

    @BaseMessage("There is no such grant defined for user ''{0}'' on host ''{1}''")
    ExInst<DingoSqlException> noDbGrantsForRevoke(String a0, String a1);

    @BaseMessage("There is no such grant defined for user ''{0}'' on host ''{1}'' on table ''{2}''")
    ExInst<DingoSqlException> noTableGrantsForRevoke(String a0, String a1, String a2);

    @BaseMessage("Operation CREATE USER failed for ''{0}''@''{1}''")
    ExInst<DingoSqlException> createUserFailed(String a0, String a1);

    @BaseMessage("Operation DROP USER failed for ''{0}''@''{1}''")
    ExInst<DingoSqlException> dropUserFailed(String a0, String a1);

    @BaseMessage("Operation DROP USER failed for ''{0}''@''{1}''")
    ExInst<DingoSqlException> alterUserFailed(String a0, String a1);

    @BaseMessage("Can't find any matching row in the user table")
    ExInst<DingoSqlException> noMatchingRowForUser();

    @BaseMessage("Access denied for user ''{0}''@''{1}''")
    ExInst<DingoSqlException> accessDeniedToUser(String a0, String a1);

    @BaseMessage("Access denied for user ''{0}''@''{1}'' to database ''{2}''")
    ExInst<DingoSqlException> accessDeniedToDb(String a0, String a1, String a2);

    @BaseMessage("Error 1051(42S02): Unknown schema ''{0}''")
    ExInst<DingoSqlException> unknownSchema(String a0);
    
    @BaseMessage("Unknown character set ''{0}''")
    ExInst<DingoSqlException> unknownCharacterSet(String a0);

    @BaseMessage("Error 1051(42S02): Unknown table ''{0}''")
    ExInst<DingoSqlException> unknownTable(String a0);
}
