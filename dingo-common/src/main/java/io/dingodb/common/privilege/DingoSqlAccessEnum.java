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

public enum DingoSqlAccessEnum {
    SELECT("select"),
    UPDATE("update"),
    INSERT("insert"),
    DELETE("delete"),
    INDEX("index"),
    ALTER("alter"),
    CREATE("create"),
    DROP("drop"),
    GRANT("grant"),
    CREATE_VIEW("create_view"),
    SHOW_VIEW("show_view"),
    CREATE_ROUTINE("create_routine"),
    ALTER_ROUTINE("alter_routine"),
    EXECUTE("execute"),
    TRIGGER("trigger"),
    EVENT("event"),
    CREATE_TMP_TABLE("create_tmp_table"),
    LOCK_TABLES("lock_tables"),
    REFERENCES("references"),
    RELOAD("reload"),
    SHUTDOWN("shutdown"),
    PROCESS("process"),
    FILE("file"),
    SHOW_DB("show_db"),
    SUPER("super"),
    REPL_SLAVE("repl_slave"),
    REPL_CLIENT("repl_client"),
    CREATE_USER("create_user"),
    CREATE_TABLESPACE("create_tablespace"),
    EXTEND1("extend1"),
    EXTEND2("extend2"),
    EXTEND3("extend3"),
    EXTEND4("extend4"),
    EXTEND5("extend5");

    DingoSqlAccessEnum(String accessType) {
        this.accessType = accessType;
    }

    private String accessType;

    public String getAccessType() {
        return accessType;
    }

    public static DingoSqlAccessEnum findEnumByName(String name) {
        for (DingoSqlAccessEnum accessEnum : DingoSqlAccessEnum.values()) {
            if (accessEnum.accessType.equalsIgnoreCase(name)) {
                return accessEnum;
            }

        }
        throw new IllegalArgumentException("name is not support");
    }
}
