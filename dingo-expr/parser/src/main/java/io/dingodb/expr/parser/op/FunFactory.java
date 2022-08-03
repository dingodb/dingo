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
import io.dingodb.expr.runtime.op.sql.RtSqlArrayConstructorOp;
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

import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

public final class FunFactory {
    public static final FunFactory INS = new FunFactory();

    private final Map<String, Supplier<Op>> funSuppliers;

    private FunFactory() {
        funSuppliers = new TreeMap<>(String::compareToIgnoreCase);

        // Logical functions are special for short-circuit processing.
        funSuppliers.put("OR", OrOp::fun);
        funSuppliers.put("AND", AndOp::fun);
        funSuppliers.put("CASE", CaseOp::fun);
        funSuppliers.put("IS_NULL", IsNullOp::fun);
        funSuppliers.put("IS_NOT_NULL", IsNotNullOp::fun);
        funSuppliers.put("IS_TRUE", IsTrueOp::fun);
        funSuppliers.put("IS_NOT_TRUE", IsNotTrueOp::fun);
        funSuppliers.put("IS_FALSE", IsFalseOp::fun);
        funSuppliers.put("IS_NOT_FALSE", IsNotFalseOp::fun);

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
        registerEvaluator(TypeCode.nameOf(TypeCode.INT), IntegerCastEvaluatorFactory.INSTANCE);
        registerEvaluator(TypeCode.nameOf(TypeCode.LONG), LongCastEvaluatorFactory.INSTANCE);
        registerEvaluator(TypeCode.nameOf(TypeCode.DOUBLE), DoubleCastEvaluatorFactory.INSTANCE);
        registerEvaluator(TypeCode.nameOf(TypeCode.DECIMAL), DecimalCastEvaluatorFactory.INSTANCE);
        registerEvaluator(TypeCode.nameOf(TypeCode.STRING), StringCastEvaluatorFactory.INSTANCE);
        registerEvaluator(TypeCode.nameOf(TypeCode.DATE), DateCastEvaluatorFactory.INSTANCE);
        registerEvaluator(TypeCode.nameOf(TypeCode.TIME), TimeCastEvaluatorFactory.INSTANCE);
        registerEvaluator(TypeCode.nameOf(TypeCode.TIMESTAMP), TimestampCastEvaluatorFactory.INSTANCE);
        registerEvaluator(TypeCode.nameOf(TypeCode.BOOL), BooleanCastEvaluatorFactory.INSTANCE);
        registerEvaluator(TypeCode.nameOf(TypeCode.BINARY), BinaryCastEvaluatorFactory.INSTANCE);

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

        // Array
        registerUdf("array", RtSqlArrayConstructorOp::new);
    }

    private void registerEvaluator(
        String funName,
        final EvaluatorFactory factory
    ) {
        funSuppliers.put(funName, () -> new OpWithEvaluator(funName, factory));
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
    @Nonnull
    public Op getFun(@Nonnull String funName) {
        Supplier<Op> supplier = funSuppliers.get(funName);
        if (supplier != null) {
            return supplier.get();
        }
        throw new UndefinedFunctionName(funName);
    }

    @Nonnull
    public Op getCastFun(int typeCode) {
        return getFun(TypeCode.nameOf(typeCode));
    }
}
