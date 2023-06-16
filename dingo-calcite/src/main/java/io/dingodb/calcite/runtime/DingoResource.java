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

    @BaseMessage("''{0}'' command denied to user ''{1}''@''{2}'' for table ''{3}''")
    ExInst<DingoSqlException> operatorDenied(String a0, String a1, String a2, String a3);

    @BaseMessage("Access denied; you need (at least one of) the CREATE USER privilege(s) for this operation")
    ExInst<DingoSqlException> createUserDenied();

    @BaseMessage("Invalid default value for ''{0}''")
    ExInst<DingoSqlException> invalidDefaultValue(String a0);

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

    @BaseMessage("You have an error in your SQL syntax")
    ExInst<DingoSqlException> parseSqlSyntaxError();

}
