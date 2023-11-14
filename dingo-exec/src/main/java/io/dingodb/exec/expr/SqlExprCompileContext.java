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
import lombok.Getter;
import lombok.ToString;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@ToString(of = {"tupleType", "parasType"})
public class SqlExprCompileContext implements CompileContext {
    public static final String TUPLE_VAR_NAME = "_";
    public static final String SQL_DYNAMIC_VAR_NAME = "_P";

    @Getter
    private final DingoType tupleType;
    private final DingoType parasType;

    public SqlExprCompileContext(DingoType tupleType, @Nullable DingoType parasType) {
        this.tupleType = tupleType;
        this.parasType = parasType;
        // Reset paras id to negative numbers.
        if (parasType != null) {
            for (int i = 0; i < parasType.fieldCount(); ++i) {
                parasType.getChild(i).setId(-i - 1);
            }
        }
    }

    @Override
    public CompileContext getChild(@NonNull Object index) {
        if (index.equals(TUPLE_VAR_NAME)) {
            return tupleType;
        } else if (index.equals(SQL_DYNAMIC_VAR_NAME)) {
            return parasType;
        }
        return null;
    }
}
