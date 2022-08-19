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

package io.dingodb.exec.expr;

import io.dingodb.common.type.DingoType;
import io.dingodb.expr.runtime.CompileContext;
import lombok.ToString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@ToString(of = {"tupleType", "parasCompileContext"})
public class SqlExprCompileContext implements CompileContext {
    public static final String SQL_DYNAMIC_VAR_NAME = "_P";
    public static final String SQL_TUPLE_VAR_NAME = "_";

    private final DingoType tupleType;
    private final CompileContext parasCompileContext;

    public SqlExprCompileContext(DingoType tupleType) {
        this(tupleType, null);
    }

    public SqlExprCompileContext(DingoType tupleType, @Nullable CompileContext parasCompileContext) {
        this.tupleType = tupleType;
        this.parasCompileContext = parasCompileContext;
    }

    @Override
    public CompileContext getChild(@Nonnull Object index) {
        if (index.equals(SQL_TUPLE_VAR_NAME)) {
            return tupleType;
        } else if (index.equals(SQL_DYNAMIC_VAR_NAME)) {
            return parasCompileContext;
        }
        return null;
    }
}
