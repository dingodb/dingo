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

import io.dingodb.expr.rel.TupleCompileContext;
import io.dingodb.expr.rel.TupleCompileContextImpl;
import io.dingodb.expr.runtime.CompileContext;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.type.TupleType;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

@ToString(of = {"tupleType", "parasType"})
@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public final class DingoCompileContext implements TupleCompileContext {
    public static final String TUPLE_VAR_NAME = "_";
    public static final String SQL_DYNAMIC_VAR_NAME = "_P";

    @Getter
    private final TupleType tupleType;
    @Getter
    private final TupleType parasType;

    public static @NonNull Expr createTupleVar(int index) {
        return Exprs.op(Exprs.INDEX, Exprs.var(TUPLE_VAR_NAME), index);
    }

    @Override
    public TupleType getType() {
        return tupleType;
    }

    @Override
    public @Nullable CompileContext getChild(@NonNull Object index) {
        if (index.equals(TUPLE_VAR_NAME)) {
            return new TupleCompileContextImpl(tupleType);
        } else if (index.equals(SQL_DYNAMIC_VAR_NAME)) {
            return new ParasCompileContext(parasType);
        }
        return null;
    }

    @Override
    public @NonNull TupleCompileContext withType(TupleType tupleType) {
        return new DingoCompileContext(tupleType, parasType);
    }
}
