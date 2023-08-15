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

package io.dingodb.expr.runtime.eval.value;

import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.runtime.eval.Eval;
import io.dingodb.expr.runtime.eval.EvalVisitor;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.nullness.qual.NonNull;

@RequiredArgsConstructor(access = AccessLevel.PUBLIC)
public final class DoubleValue implements Eval {
    private static final long serialVersionUID = -3661539645046160093L;

    @Getter
    private final Double value;

    @Override
    public int getType() {
        return TypeCode.DOUBLE;
    }

    @Override
    public <T> T accept(@NonNull EvalVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
