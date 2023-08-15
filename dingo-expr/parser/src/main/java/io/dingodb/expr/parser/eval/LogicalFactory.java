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

package io.dingodb.expr.parser.eval;

import io.dingodb.expr.runtime.eval.Eval;
import io.dingodb.expr.runtime.eval.logical.AndEval;
import io.dingodb.expr.runtime.eval.logical.NotEval;
import io.dingodb.expr.runtime.eval.logical.OrEval;
import io.dingodb.expr.runtime.eval.logical.VarArgAndEval;
import io.dingodb.expr.runtime.eval.logical.VarArgOrEval;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

public final class LogicalFactory {
    private LogicalFactory() {
    }

    public static @NonNull NotEval not(@NonNull Eval operand) {
        return new NotEval(CastingFactory.toBool(operand));
    }

    public static @NonNull AndEval and(@NonNull Eval operand0, @NonNull Eval operand1) {
        return new AndEval(CastingFactory.toBool(operand0), CastingFactory.toBool(operand1));
    }

    public static @NonNull OrEval or(@NonNull Eval operand0, @NonNull Eval operand1) {
        return new OrEval(CastingFactory.toBool(operand0), CastingFactory.toBool(operand1));
    }

    public static @NonNull VarArgAndEval varAnd(@NonNull List<Eval> operands) {
        return new VarArgAndEval(operands.stream().map(CastingFactory::toBool).toArray(Eval[]::new));
    }

    public static @NonNull VarArgOrEval varOr(@NonNull List<Eval> operands) {
        return new VarArgOrEval(operands.stream().map(CastingFactory::toBool).toArray(Eval[]::new));
    }
}
