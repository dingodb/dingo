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

package io.dingodb.exec.fun;

import io.dingodb.exec.fun.like.LikeBinaryOp;
import io.dingodb.exec.fun.like.LikeOp;
import io.dingodb.exec.fun.number.AbsEvaluatorsFactory;
import io.dingodb.exec.fun.number.CeilEvaluatorsFactory;
import io.dingodb.exec.fun.number.FloorEvaluatorsFactory;
import io.dingodb.exec.fun.number.FormatFun;
import io.dingodb.exec.fun.number.ModEvaluatorsFactory;
import io.dingodb.exec.fun.number.PowFun;
import io.dingodb.exec.fun.number.RoundEvaluatorsFactory;
import io.dingodb.exec.fun.special.ArrayConstructorOp;
import io.dingodb.exec.fun.special.CaseOp;
import io.dingodb.exec.fun.special.CastListItemsOp;
import io.dingodb.exec.fun.special.ItemEvaluatorsFactory;
import io.dingodb.exec.fun.special.ListConstructorOp;
import io.dingodb.exec.fun.special.MapConstructorOp;
import io.dingodb.exec.fun.special.RtSliceFun;
import io.dingodb.exec.fun.string.CharLengthFun;
import io.dingodb.exec.fun.string.ConcatFun;
import io.dingodb.exec.fun.string.LTrimFun;
import io.dingodb.exec.fun.string.LeftFun;
import io.dingodb.exec.fun.string.LocateFun;
import io.dingodb.exec.fun.string.MidFun;
import io.dingodb.exec.fun.string.RTrimFun;
import io.dingodb.exec.fun.string.RepeatFun;
import io.dingodb.exec.fun.string.ReplaceFun;
import io.dingodb.exec.fun.string.ReverseFun;
import io.dingodb.exec.fun.string.RightFun;
import io.dingodb.exec.fun.string.SubstringFun;
import io.dingodb.exec.fun.string.TrimFun;
import io.dingodb.exec.fun.time.CurrentDateFun;
import io.dingodb.exec.fun.time.CurrentTimeFun;
import io.dingodb.exec.fun.time.CurrentTimestampFun;
import io.dingodb.exec.fun.time.DateDiffFun;
import io.dingodb.exec.fun.time.DateFormatEvaluatorsFactory;
import io.dingodb.exec.fun.time.FromUnixTimeEvaluatorsFactory;
import io.dingodb.exec.fun.time.TimeFormatEvaluatorsFactory;
import io.dingodb.exec.fun.time.TimestampFormatEvaluatorsFactory;
import io.dingodb.exec.fun.time.UnixTimestampEvaluatorsFactory;
import io.dingodb.expr.parser.DefaultFunFactory;
import io.dingodb.expr.runtime.evaluator.index.IndexEvaluatorsFactory;

public class DingoFunFactory extends DefaultFunFactory {
    // number functions
    public static final String ABS = "abs";
    public static final String CEIL = "ceil";
    public static final String FLOOR = "floor";
    public static final String MOD = "mod";
    public static final String ROUND = "round";
    // special function
    public static final String ITEM = "item";
    // date & time functions
    public static final String DATE_FORMAT = "date_format";
    public static final String TIME_FORMAT = "time_format";
    public static final String TIMESTAMP_FORMAT = "timestamp_format";
    public static final String FROM_UNIXTIME = "from_unixtime";
    public static final String UNIX_TIMESTAMP = "unix_timestamp";

    private static DingoFunFactory instance;

    private DingoFunFactory() {
        super();
    }

    public static synchronized DingoFunFactory getInstance() {
        if (instance == null) {
            instance = new DingoFunFactory();
            instance.init();
        }
        return instance;
    }

    private void init() {
        // like
        registerUdf(LikeBinaryOp.NAME, LikeBinaryOp::new);
        registerUdf(LikeOp.NAME, LikeOp::new);
        // number
        registerEvaluator(ABS, AbsEvaluatorsFactory.INSTANCE);
        registerEvaluator(CEIL, CeilEvaluatorsFactory.INSTANCE);
        registerEvaluator(FLOOR, FloorEvaluatorsFactory.INSTANCE);
        registerUdf(FormatFun.NAME, FormatFun::new);
        registerEvaluator(MOD, ModEvaluatorsFactory.INSTANCE);
        registerUdf(PowFun.NAME, PowFun::new);
        registerEvaluator(ROUND, RoundEvaluatorsFactory.INSTANCE);
        // special
        funSuppliers.put(ArrayConstructorOp.NAME, ArrayConstructorOp::fun);
        funSuppliers.put(ListConstructorOp.NAME, ListConstructorOp::fun);
        funSuppliers.put(MapConstructorOp.NAME, MapConstructorOp::fun);
        funSuppliers.put(CastListItemsOp.NAME, CastListItemsOp::fun);
        funSuppliers.put(CaseOp.NAME, CaseOp::fun);
        registerUdf(RtSliceFun.NAME, RtSliceFun::new);
        registerEvaluator(ITEM, ItemEvaluatorsFactory.INSTANCE);
        // string
        registerUdf(CharLengthFun.NAME, CharLengthFun::new);
        registerUdf(ConcatFun.NAME, ConcatFun::new);
        registerUdf(LeftFun.NAME, LeftFun::new);
        registerUdf(LocateFun.NAME, LocateFun::new);
        registerUdf(LTrimFun.NAME, LTrimFun::new);
        registerUdf(MidFun.NAME, MidFun::new);
        registerUdf(RepeatFun.NAME, RepeatFun::new);
        registerUdf(ReplaceFun.NAME, ReplaceFun::new);
        registerUdf(ReverseFun.NAME, ReverseFun::new);
        registerUdf(RightFun.NAME, RightFun::new);
        registerUdf(RTrimFun.NAME, RTrimFun::new);
        registerUdf(SubstringFun.NAME, SubstringFun::new);
        registerUdf(TrimFun.NAME, TrimFun::new);
        // time
        registerUdf(CurrentDateFun.NAME, CurrentDateFun::new);
        registerUdf(CurrentTimeFun.NAME, CurrentTimeFun::new);
        registerUdf(CurrentTimestampFun.NAME, CurrentTimestampFun::new);
        registerEvaluator(FROM_UNIXTIME, FromUnixTimeEvaluatorsFactory.INSTANCE);
        registerEvaluator(UNIX_TIMESTAMP, UnixTimestampEvaluatorsFactory.INSTANCE);
        registerUdf(DateDiffFun.NAME, DateDiffFun::new);
        registerEvaluator(DATE_FORMAT, DateFormatEvaluatorsFactory.INSTANCE);
        registerEvaluator(TIME_FORMAT, TimeFormatEvaluatorsFactory.INSTANCE);
        registerEvaluator(TIMESTAMP_FORMAT, TimestampFormatEvaluatorsFactory.INSTANCE);
    }
}
