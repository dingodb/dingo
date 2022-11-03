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

package io.dingodb.expr.parser;

import io.dingodb.expr.core.TypeCode;
import io.dingodb.expr.core.evaluator.EvaluatorFactory;
import io.dingodb.expr.parser.exception.UndefinedFunctionName;
import io.dingodb.expr.parser.op.AndOp;
import io.dingodb.expr.parser.op.IsFalseOp;
import io.dingodb.expr.parser.op.IsNotFalseOp;
import io.dingodb.expr.parser.op.IsNotNullOp;
import io.dingodb.expr.parser.op.IsNotTrueOp;
import io.dingodb.expr.parser.op.IsNullOp;
import io.dingodb.expr.parser.op.IsTrueOp;
import io.dingodb.expr.parser.op.Op;
import io.dingodb.expr.parser.op.OpWithEvaluator;
import io.dingodb.expr.parser.op.OrOp;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.evaluator.arithmetic.MaxEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.arithmetic.MinEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.cast.BinaryCastEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.cast.BooleanCastEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.cast.DateCastEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.cast.DecimalCastEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.cast.DoubleCastEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.cast.IntegerCastEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.cast.LongCastEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.cast.StringCastEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.cast.TimeCastEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.cast.TimestampCastEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.AbsEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.AcosEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.AsinEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.AtanEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.CosEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.CoshEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.ExpEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.LogEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.SinEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.SinhEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.TanEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.TanhEvaluatorFactory;
import io.dingodb.expr.runtime.op.RtOp;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;

public class DefaultFunFactory implements FunFactory {
    protected final Map<String, Supplier<Op>> funSuppliers;

    public DefaultFunFactory() {
        funSuppliers = new TreeMap<>(String::compareToIgnoreCase);

        // Logical functions are special for short-circuit processing.
        funSuppliers.put(OrOp.FUN_NAME, OrOp::fun);
        funSuppliers.put(AndOp.FUN_NAME, AndOp::fun);
        funSuppliers.put(IsNullOp.FUN_NAME, IsNullOp::fun);
        funSuppliers.put(IsNotNullOp.FUN_NAME, IsNotNullOp::fun);
        funSuppliers.put(IsTrueOp.FUN_NAME, IsTrueOp::fun);
        funSuppliers.put(IsNotTrueOp.FUN_NAME, IsNotTrueOp::fun);
        funSuppliers.put(IsFalseOp.FUN_NAME, IsFalseOp::fun);
        funSuppliers.put(IsNotFalseOp.FUN_NAME, IsNotFalseOp::fun);

        // min, max
        registerEvaluator("min", MinEvaluatorFactory.INSTANCE);
        registerEvaluator("max", MaxEvaluatorFactory.INSTANCE);

        // Mathematical
        registerEvaluator("abs", AbsEvaluatorFactory.INSTANCE);
        registerEvaluator("sin", SinEvaluatorFactory.INSTANCE);
        registerEvaluator("cos", CosEvaluatorFactory.INSTANCE);
        registerEvaluator("tan", TanEvaluatorFactory.INSTANCE);
        registerEvaluator("asin", AsinEvaluatorFactory.INSTANCE);
        registerEvaluator("acos", AcosEvaluatorFactory.INSTANCE);
        registerEvaluator("atan", AtanEvaluatorFactory.INSTANCE);
        registerEvaluator("cosh", CoshEvaluatorFactory.INSTANCE);
        registerEvaluator("sinh", SinhEvaluatorFactory.INSTANCE);
        registerEvaluator("tanh", TanhEvaluatorFactory.INSTANCE);
        registerEvaluator("log", LogEvaluatorFactory.INSTANCE);
        registerEvaluator("exp", ExpEvaluatorFactory.INSTANCE);

        // Cast functions
        registerCastFun(TypeCode.INT);
        registerCastFun(TypeCode.LONG);
        registerCastFun(TypeCode.DOUBLE);
        registerCastFun(TypeCode.BOOL);
        registerCastFun(TypeCode.DECIMAL);
        registerCastFun(TypeCode.STRING);
        registerCastFun(TypeCode.DATE);
        registerCastFun(TypeCode.TIME);
        registerCastFun(TypeCode.TIMESTAMP);
        registerCastFun(TypeCode.BINARY);
    }

    public static EvaluatorFactory getCastEvaluatorFactory(int toTypeCode) {
        switch (toTypeCode) {
            case TypeCode.INT:
                return IntegerCastEvaluatorFactory.INSTANCE;
            case TypeCode.LONG:
                return LongCastEvaluatorFactory.INSTANCE;
            case TypeCode.DOUBLE:
                return DoubleCastEvaluatorFactory.INSTANCE;
            case TypeCode.BOOL:
                return BooleanCastEvaluatorFactory.INSTANCE;
            case TypeCode.DECIMAL:
                return DecimalCastEvaluatorFactory.INSTANCE;
            case TypeCode.STRING:
                return StringCastEvaluatorFactory.INSTANCE;
            case TypeCode.DATE:
                return DateCastEvaluatorFactory.INSTANCE;
            case TypeCode.TIME:
                return TimeCastEvaluatorFactory.INSTANCE;
            case TypeCode.TIMESTAMP:
                return TimestampCastEvaluatorFactory.INSTANCE;
            case TypeCode.BINARY:
                return BinaryCastEvaluatorFactory.INSTANCE;
        }
        throw new IllegalArgumentException("Unsupported cast type: \"" + castFunName(toTypeCode) + "\".");
    }

    @NonNull
    public static String castFunName(int typeCode) {
        return TypeCode.nameOf(typeCode);
    }

    public void registerEvaluator(String funName, final EvaluatorFactory factory) {
        funSuppliers.put(funName, () -> new OpWithEvaluator(funName, factory));
    }

    private void registerCastFun(int typeCode) {
        registerEvaluator(castFunName(typeCode), getCastEvaluatorFactory(typeCode));
    }

    @Override
    public @NonNull Op getFun(String funName) {
        Supplier<Op> supplier = funSuppliers.get(funName);
        if (supplier != null) {
            return supplier.get();
        }
        throw new UndefinedFunctionName(funName);
    }

    @Override
    public void registerUdf(@NonNull String funName, final @NonNull Function<RtExpr[], RtOp> funSupplier) {
        funSuppliers.put(funName, () -> new RtOpWrapper(funName, funSupplier));
    }

    public @NonNull Op getCastFun(int typeCode) {
        return getFun(castFunName(typeCode));
    }
}
