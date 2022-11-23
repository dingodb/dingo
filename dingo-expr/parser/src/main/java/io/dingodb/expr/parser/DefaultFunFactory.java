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
import io.dingodb.expr.runtime.evaluator.cast.BinaryCastEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.cast.BooleanCastEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.cast.DateCastEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.cast.DecimalCastEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.cast.DoubleCastEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.cast.IntCastEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.cast.IntCastRCEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.cast.LongCastEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.cast.LongCastRCEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.cast.StringCastEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.cast.TimeCastEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.cast.TimestampCastEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.AbsEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.AcosEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.AsinEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.AtanEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.CosEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.CoshEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.ExpEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.LogEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.MaxEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.MinEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.SinEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.SinhEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.TanEvaluatorsFactory;
import io.dingodb.expr.runtime.evaluator.mathematical.TanhEvaluatorsFactory;
import io.dingodb.expr.runtime.op.RtOp;
import io.dingodb.expr.runtime.op.string.RtLowerFun;
import io.dingodb.expr.runtime.op.string.RtReplaceFun;
import io.dingodb.expr.runtime.op.string.RtTrimFun;
import io.dingodb.expr.runtime.op.string.RtUpperFun;
import io.dingodb.expr.runtime.op.string.SubstrEvaluatorsFactory;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.ServiceLoader;
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

        // Mathematical
        registerEvaluator("min", MinEvaluatorsFactory.INSTANCE);
        registerEvaluator("max", MaxEvaluatorsFactory.INSTANCE);
        registerEvaluator("abs", AbsEvaluatorsFactory.INSTANCE);
        registerEvaluator("sin", SinEvaluatorsFactory.INSTANCE);
        registerEvaluator("cos", CosEvaluatorsFactory.INSTANCE);
        registerEvaluator("tan", TanEvaluatorsFactory.INSTANCE);
        registerEvaluator("asin", AsinEvaluatorsFactory.INSTANCE);
        registerEvaluator("acos", AcosEvaluatorsFactory.INSTANCE);
        registerEvaluator("atan", AtanEvaluatorsFactory.INSTANCE);
        registerEvaluator("cosh", CoshEvaluatorsFactory.INSTANCE);
        registerEvaluator("sinh", SinhEvaluatorsFactory.INSTANCE);
        registerEvaluator("tanh", TanhEvaluatorsFactory.INSTANCE);
        registerEvaluator("log", LogEvaluatorsFactory.INSTANCE);
        registerEvaluator("exp", ExpEvaluatorsFactory.INSTANCE);

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

        // String functions
        registerUdf("lower", RtLowerFun::new);
        registerUdf("upper", RtUpperFun::new);
        registerUdf("trim", RtTrimFun::new);
        registerUdf("replace", RtReplaceFun::new);
        registerEvaluator("substr", SubstrEvaluatorsFactory.INSTANCE);
    }

    public static EvaluatorFactory getCastEvaluatorFactory(int toTypeCode) {
        return getCastEvaluatorFactory(toTypeCode, false);
    }

    public static EvaluatorFactory getCastEvaluatorFactory(int toTypeCode, boolean checkRange) {
        switch (toTypeCode) {
            case TypeCode.INT:
                return checkRange ? IntCastRCEvaluatorsFactory.INSTANCE : IntCastEvaluatorsFactory.INSTANCE;
            case TypeCode.LONG:
                return checkRange ? LongCastRCEvaluatorsFactory.INSTANCE : LongCastEvaluatorsFactory.INSTANCE;
            case TypeCode.DOUBLE:
                return DoubleCastEvaluatorsFactory.INSTANCE;
            case TypeCode.BOOL:
                return BooleanCastEvaluatorsFactory.INSTANCE;
            case TypeCode.DECIMAL:
                return DecimalCastEvaluatorsFactory.INSTANCE;
            case TypeCode.STRING:
                return StringCastEvaluatorsFactory.INSTANCE;
            case TypeCode.DATE:
                return DateCastEvaluatorsFactory.INSTANCE;
            case TypeCode.TIME:
                return TimeCastEvaluatorsFactory.INSTANCE;
            case TypeCode.TIMESTAMP:
                return TimestampCastEvaluatorsFactory.INSTANCE;
            case TypeCode.BINARY:
                return BinaryCastEvaluatorsFactory.INSTANCE;
            default:
                throw new IllegalArgumentException("Unsupported cast type: \"" + castFunName(toTypeCode) + "\".");
        }
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
