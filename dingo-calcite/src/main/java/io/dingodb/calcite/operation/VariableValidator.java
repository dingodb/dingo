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

package io.dingodb.calcite.operation;

import io.dingodb.common.mysql.constant.ErrorCode;
import io.dingodb.common.mysql.scope.ScopeVariables;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static io.dingodb.calcite.runtime.DingoResource.DINGO_RESOURCE;

public final class VariableValidator {

    private static final List<Object> SWITCH = new ArrayList<>();
    private static final List<Object> TX_ISOLATION = new ArrayList<>();
    private static final List<Object> TX_MODE = new ArrayList<>();

    static {
        SWITCH.add("0");
        SWITCH.add("1");
        SWITCH.add("on");
        SWITCH.add("off");
        //TX_ISOLATION.add("read-uncommitted");
        TX_ISOLATION.add("read-committed");
        TX_ISOLATION.add("repeatable-read");
        //TX_ISOLATION.add("serializable");
        TX_MODE.add("pessimistic");
        TX_MODE.add("optimistic");
    }

    private VariableValidator() {
    }

    public static String validator(String name, String value, String scope) {
        if (ScopeVariables.immutableVariables.contains(name)) {
            throw new RuntimeException(String.format(ErrorCode.ER_IMMUTABLE_VARIABLES.message, name));
        }
        if (name.equalsIgnoreCase("autocommit")
            || name.equalsIgnoreCase("transaction_read_only")
            || name.equalsIgnoreCase("txn_inert_check")
            || name.equalsIgnoreCase("txn_retry")
        ) {
            value = value.toLowerCase();
            if (!SWITCH.contains(value)) {
                throw DINGO_RESOURCE.invalidVariableArg(name, value).ex();
            }
            String on = transformSwitch(value);
            if (on != null) {
                value = on;
            }
        } else if (name.equalsIgnoreCase("transaction_isolation")) {
            if (!TX_ISOLATION.contains(value.toLowerCase())) {
                throw DINGO_RESOURCE.invalidVariableArg(name, value).ex();
            }
            value = value.toUpperCase();
        } else if (name.equalsIgnoreCase("txn_mode")) {
            value = value.toLowerCase();
            if (!TX_MODE.contains(value)) {
                throw DINGO_RESOURCE.invalidVariableArg(name, value).ex();
            }
        } else if (name.endsWith("timeout")
            || name.equalsIgnoreCase("txn_retry_cnt")
            || name.equalsIgnoreCase("max_execution_time")
        ) {
            if (!value.matches("\\d+")) {
                throw DINGO_RESOURCE.incorrectArgType(name).ex();
            }
        }

        if ("SYSTEM".equals(scope)) {
            if (!ScopeVariables.globalVariables.containsKey(name)) {
                throw new RuntimeException(String.format(ErrorCode.ER_UNKNOWN_VARIABLES.message, name));
            }
        } else {
            if (value.contains("'")) {
                value = value.replace("'", "");
            }
        }
        return value;
    }

    @Nullable
    private static String transformSwitch(String value) {
        if (value.equals("1")) {
            return "on";
        } else if (value.equals("0")) {
            return "off";
        }
        return null;
    }
}
