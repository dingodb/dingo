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

package io.dingodb.exec.fun.mysql;

import io.dingodb.common.mysql.scope.ScopeVariables;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.op.UnaryOp;
import io.dingodb.expr.runtime.type.Type;
import io.dingodb.expr.runtime.type.Types;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class GlobalVariableFun extends UnaryOp {
    public static final GlobalVariableFun INSTANCE = new GlobalVariableFun();
    public static final String NAME = "@@";

    private static final long serialVersionUID = 5242457055774200529L;

    @Override
    protected Object evalNonNullValue(@NonNull Object value, ExprConfig config) {
        String str = (String) value;
        str = str.toLowerCase();
        if (str.startsWith("session.")) {
            str = str.substring(8);
            return ScopeVariables.sessionVariables.getOrDefault(str, "");
        } else if (str.startsWith("global.")) {
            str = str.substring(7);
            return ScopeVariables.globalVariables.getOrDefault(str, "");
        } else {
            return ScopeVariables.globalVariables.getOrDefault(str, "");
        }
    }

    @Override
    public Object keyOf(@NonNull Type type) {
        if (type.equals(Types.STRING)) {
            return Types.STRING;
        }
        return null;
    }

    @Override
    public UnaryOp getOp(Object key) {
        return (key != null && key.equals(Types.STRING)) ? this : null;
    }
}
