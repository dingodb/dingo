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
import io.dingodb.expr.runtime.expr.Val;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Set;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
class ValSelectionVisitor extends TypeVisitorBase<SelectionFlag, Set<Integer>> {

    private final Val val;

    @SneakyThrows
    @Override
    public SelectionFlag visitIntType(@NonNull IntType type, Set<Integer> selected) {
        Integer value = (Integer) val.getValue();
        if (value != null) {
            selected.add(value);
        }
        return SelectionFlag.OK;
    }

    @SneakyThrows
    @Override
    public SelectionFlag visitLongType(@NonNull LongType type, Set<Integer> selected) {
        Long value = (Long) val.getValue();
        if (value != null) {
            selected.add(Math.toIntExact(value));
        }
        return SelectionFlag.OK;
    }

    @SneakyThrows
    @Override
    public SelectionFlag visitDateType(@NonNull DateType type, Set<Integer> selected) {
        return SelectionFlag.OK;
    }

    @SneakyThrows
    @Override
    public SelectionFlag visitFloatType(@NonNull FloatType type, Set<Integer> selected) {
        return SelectionFlag.OK;
    }

    @SneakyThrows
    @Override
    public SelectionFlag visitDoubleType(@NonNull DoubleType type, Set<Integer> selected) {
        return SelectionFlag.OK;
    }

    @SneakyThrows
    @Override
    public SelectionFlag visitBoolType(@NonNull BoolType type, Set<Integer> selected) {
        return SelectionFlag.OK;
    }

    @SneakyThrows
    @Override
    public SelectionFlag visitStringType(@NonNull StringType type, Set<Integer> selected) {
        return SelectionFlag.OK;
    }

    @Override
    public SelectionFlag visitArrayType(@NonNull ArrayType type, Set<Integer> selected) {
        return SelectionFlag.OK;
    }
}
