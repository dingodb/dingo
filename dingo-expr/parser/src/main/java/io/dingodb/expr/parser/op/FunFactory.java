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

package io.dingodb.expr.parser.op;

import io.dingodb.expr.parser.exception.UndefinedFunctionName;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.TypeCode;
import io.dingodb.expr.runtime.evaluator.arithmetic.AbsEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.arithmetic.MaxEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.arithmetic.MinEvaluatorFactory;
import io.dingodb.expr.runtime.evaluator.base.EvaluatorFactory;
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
import io.dingodb.expr.runtime.op.number.DingoNumberAbsOp;
import io.dingodb.expr.runtime.op.number.DingoNumberCeilingOp;
import io.dingodb.expr.runtime.op.number.DingoNumberFloorOp;
import io.dingodb.expr.runtime.op.number.DingoNumberFormatOp;
import io.dingodb.expr.runtime.op.number.DingoNumberModOp;
import io.dingodb.expr.runtime.op.number.DingoNumberPowOp;
import io.dingodb.expr.runtime.op.number.DingoNumberRoundOp;
import io.dingodb.expr.runtime.op.sql.RtSqlSliceFun;
import io.dingodb.expr.runtime.op.string.DingoCharLengthOp;
import io.dingodb.expr.runtime.op.string.DingoStringConcatOp;
import io.dingodb.expr.runtime.op.string.DingoStringLTrimOp;
import io.dingodb.expr.runtime.op.string.DingoStringLeftOp;
import io.dingodb.expr.runtime.op.string.DingoStringLocateOp;
import io.dingodb.expr.runtime.op.string.DingoStringLowerOp;
import io.dingodb.expr.runtime.op.string.DingoStringMidOp;
import io.dingodb.expr.runtime.op.string.DingoStringRTrimOp;
import io.dingodb.expr.runtime.op.string.DingoStringRepeatOp;
import io.dingodb.expr.runtime.op.string.DingoStringReplaceOp;
import io.dingodb.expr.runtime.op.string.DingoStringReverseOp;
import io.dingodb.expr.runtime.op.string.DingoStringRightOp;
import io.dingodb.expr.runtime.op.string.DingoStringTrimOp;
import io.dingodb.expr.runtime.op.string.DingoStringUpperOp;
import io.dingodb.expr.runtime.op.string.DingoSubStringOp;
import io.dingodb.expr.runtime.op.string.RtContainsOp;
import io.dingodb.expr.runtime.op.string.RtEndsWithOp;
import io.dingodb.expr.runtime.op.string.RtMatchesOp;
import io.dingodb.expr.runtime.op.string.RtStartsWithOp;
import io.dingodb.expr.runtime.op.time.CurrentDateFun;
import io.dingodb.expr.runtime.op.time.CurrentTimeFun;
import io.dingodb.expr.runtime.op.time.CurrentTimestampFun;
import io.dingodb.expr.runtime.op.time.DateDiffFun;
import io.dingodb.expr.runtime.op.time.DateFormatFun;
import io.dingodb.expr.runtime.op.time.FromUnixTimeFun;
import io.dingodb.expr.runtime.op.time.TimeFormatFun;
import io.dingodb.expr.runtime.op.time.TimestampFormatFun;
import io.dingodb.expr.runtime.op.time.UnixTimestampFun;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;

public final class FunFactory {
    public static final FunFactory INS = new FunFactory();

    private final Map<String, Supplier<Op>> funSuppliers;

    private FunFactory() {
        funSuppliers = new TreeMap<>(String::compareToIgnoreCase);

        // Logical functions are special for short-circuit processing.
        funSuppliers.put(OrOp.FUN_NAME, OrOp::fun);
        funSuppliers.put(AndOp.FUN_NAME, AndOp::fun);
        funSuppliers.put(SqlCaseOp.FUN_NAME, SqlCaseOp::fun);
        funSuppliers.put(SqlCastListItemsOp.FUN_NAME, SqlCastListItemsOp::fun);
        funSuppliers.put(IsNullOp.FUN_NAME, IsNullOp::fun);
        funSuppliers.put(IsNotNullOp.FUN_NAME, IsNotNullOp::fun);
        funSuppliers.put(IsTrueOp.FUN_NAME, IsTrueOp::fun);
        funSuppliers.put(IsNotTrueOp.FUN_NAME, IsNotTrueOp::fun);
        funSuppliers.put(IsFalseOp.FUN_NAME, IsFalseOp::fun);
        funSuppliers.put(IsNotFalseOp.FUN_NAME, IsNotFalseOp::fun);
        // Collection type constructors
        funSuppliers.put(SqlArrayConstructorOp.FUN_NAME, SqlArrayConstructorOp::fun);
        funSuppliers.put(SqlListConstructorOp.FUN_NAME, SqlListConstructorOp::fun);
        funSuppliers.put(SqlMapConstructorOp.FUN_NAME, SqlMapConstructorOp::fun);

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

        // String
        registerUdf("char_length", DingoCharLengthOp::new);
        registerUdf("replace", DingoStringReplaceOp::new);
        registerUdf("substring", DingoSubStringOp::new);
        registerUdf("trim", DingoStringTrimOp::new);
        registerUdf("ltrim", DingoStringLTrimOp::new);
        registerUdf("rtrim", DingoStringRTrimOp::new);
        registerUdf("lower", DingoStringLowerOp::new);
        registerUdf("lcase", DingoStringLowerOp::new);
        registerUdf("upper", DingoStringUpperOp::new);
        registerUdf("ucase", DingoStringUpperOp::new);
        registerUdf("left", DingoStringLeftOp::new);
        registerUdf("right", DingoStringRightOp::new);
        registerUdf("reverse", DingoStringReverseOp::new);
        registerUdf("repeat", DingoStringRepeatOp::new);
        registerUdf("mid", DingoStringMidOp::new);
        registerUdf("locate", DingoStringLocateOp::new);
        registerUdf("concat", DingoStringConcatOp::new);
        registerUdf("startswith", RtStartsWithOp::new);
        registerUdf("endswith", RtEndsWithOp::new);
        registerUdf("contains", RtContainsOp::new);
        registerUdf("matches", RtMatchesOp::new);

        // number format function
        registerUdf("format", DingoNumberFormatOp::new);
        registerUdf("abs", DingoNumberAbsOp::new);
        registerUdf("mod", DingoNumberModOp::new);
        registerUdf("floor", DingoNumberFloorOp::new);
        registerUdf("ceiling", DingoNumberCeilingOp::new);
        registerUdf("ceil", DingoNumberCeilingOp::new);
        registerUdf("pow", DingoNumberPowOp::new);
        registerUdf("round", DingoNumberRoundOp::new);

        // Date & time
        registerUdf("current_date", CurrentDateFun::new);
        registerUdf("curdate", CurrentDateFun::new);
        registerUdf("current_time", CurrentTimeFun::new);
        registerUdf("curtime", CurrentTimeFun::new);
        registerUdf("current_timestamp", CurrentTimestampFun::new);
        registerUdf("now", CurrentTimestampFun::new);
        registerUdf("from_unixtime", FromUnixTimeFun::new);
        registerUdf("unix_timestamp", UnixTimestampFun::new);
        registerUdf("date_format", DateFormatFun::new);
        registerUdf("time_format", TimeFormatFun::new);
        registerUdf("timestamp_format", TimestampFormatFun::new);
        registerUdf("datediff", DateDiffFun::new);

        registerUdf("$SLICE", RtSqlSliceFun::new);
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
        throw new IllegalArgumentException("Unsupported cast type: \"" + TypeCode.nameOf(toTypeCode) + "\".");
    }

    private void registerEvaluator(
        String funName,
        final EvaluatorFactory factory
    ) {
        funSuppliers.put(funName, () -> new OpWithEvaluator(funName, factory));
    }

    private void registerCastFun(int typeCode) {
        registerEvaluator(TypeCode.nameOf(typeCode), getCastEvaluatorFactory(typeCode));
    }

    /**
     * Register a user defined function.
     *
     * @param funName     the name of the function
     * @param funSupplier a function to create the runtime function object
     */
    public void registerUdf(
        String funName,
        final Function<RtExpr[], RtOp> funSupplier
    ) {
        funSuppliers.put(funName, () -> new RtOpWrapper(funName, funSupplier));
    }

    /**
     * Get the function (Op) by its name.
     *
     * @param funName the name of the function
     * @return the function (Op)
     */
    public @NonNull Op getFun(String funName) {
        Supplier<Op> supplier = funSuppliers.get(funName);
        if (supplier != null) {
            return supplier.get();
        }
        throw new UndefinedFunctionName(funName);
    }

    public @NonNull Op getCastFun(int typeCode) {
        return getFun(TypeCode.nameOf(typeCode));
    }
}
