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

package io.dingodb.common.ddl;

public enum ActionType {
    ActionNone(0),
    ActionCreateSchema(1),
    ActionDropSchema(2),
    ActionCreateTable(3),
    ActionDropTable(4),
    ActionAddColumn(5),
    ActionDropColumn(6),
    ActionAddIndex(7),
    ActionDropIndex(8),
    ActionTruncateTable(11),
    ActionModifyColumn(12),
    ActionAddPrimaryKey(32),
    ActionCreateTables(60),
    ;
    private final int code;

    ActionType(int code) {
        this.code = code;
    }

    public long getCode() {
        return code;
    }

}
