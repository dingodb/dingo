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

package io.dingodb.exec.utils.relop;

import io.dingodb.expr.common.type.ArrayType;
import io.dingodb.expr.common.type.BoolType;
import io.dingodb.expr.common.type.DateType;
import io.dingodb.expr.common.type.DoubleType;
import io.dingodb.expr.common.type.FloatType;
import io.dingodb.expr.common.type.IntType;
import io.dingodb.expr.common.type.LongType;
import io.dingodb.expr.common.type.StringType;
import io.dingodb.expr.common.type.TypeVisitorBase;
import io.dingodb.expr.runtime.expr.Expr;
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.expr.Val;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.sql.Date;
import java.util.List;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class ValMappingVisitor extends TypeVisitorBase<Expr, List<Integer>> {

    private final Val val;

    @SneakyThrows
    @Override
    public Expr visitIntType(@NonNull IntType type, List<Integer> selection) {
        Integer value = (Integer) val.getValue();
        return Exprs.val(value, type);
    }

    @SneakyThrows
    @Override
    public Expr visitLongType(@NonNull LongType type, List<Integer> selection) {
        Long value = (Long) val.getValue();
        return Exprs.val(value, type);
    }

    @SneakyThrows
    @Override
    public Expr visitDateType(@NonNull DateType type, List<Integer> selection) {
        Date value = (Date) val.getValue();
        return Exprs.val(value, type);
    }

    @SneakyThrows
    @Override
    public Expr visitFloatType(@NonNull FloatType type, List<Integer> selection) {
        Float value = (Float) val.getValue();
        return Exprs.val(value, type);
    }

    @SneakyThrows
    @Override
    public Expr visitDoubleType(@NonNull DoubleType type, List<Integer> selection) {
        Double value = (Double) val.getValue();
        return Exprs.val(value, type);
    }

    @SneakyThrows
    @Override
    public Expr visitBoolType(@NonNull BoolType type, List<Integer> selection) {
        Boolean value = (Boolean) val.getValue();
        return Exprs.val(value, type);
    }

    @SneakyThrows
    @Override
    public Expr visitStringType(@NonNull StringType type, List<Integer> selection) {
        String value = (String) val.getValue();
        return Exprs.val(value, type);
    }

    @Override
    public Expr visitArrayType(@NonNull ArrayType type, List<Integer> selection) {
        return Exprs.val(val.getValue(), type);
    }
}
