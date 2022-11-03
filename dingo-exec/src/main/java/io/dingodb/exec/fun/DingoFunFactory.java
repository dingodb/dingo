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
import io.dingodb.exec.fun.number.CeilFun;
import io.dingodb.exec.fun.number.FloorFun;
import io.dingodb.exec.fun.number.FormatFun;
import io.dingodb.exec.fun.number.ModFun;
import io.dingodb.exec.fun.number.PowFun;
import io.dingodb.exec.fun.number.RoundFun;
import io.dingodb.exec.fun.special.ArrayConstructorOp;
import io.dingodb.exec.fun.special.CaseOp;
import io.dingodb.exec.fun.special.CastListItemsOp;
import io.dingodb.exec.fun.special.ListConstructorOp;
import io.dingodb.exec.fun.special.MapConstructorOp;
import io.dingodb.exec.fun.special.RtSliceFun;
import io.dingodb.exec.fun.string.CharLengthFun;
import io.dingodb.exec.fun.string.ConcatFun;
import io.dingodb.exec.fun.string.LTrimFun;
import io.dingodb.exec.fun.string.LeftFun;
import io.dingodb.exec.fun.string.LocateFun;
import io.dingodb.exec.fun.string.LowerFun;
import io.dingodb.exec.fun.string.MidFun;
import io.dingodb.exec.fun.string.RTrimFun;
import io.dingodb.exec.fun.string.RepeatFun;
import io.dingodb.exec.fun.string.ReplaceFun;
import io.dingodb.exec.fun.string.ReverseFun;
import io.dingodb.exec.fun.string.RightFun;
import io.dingodb.exec.fun.string.SubstringFun;
import io.dingodb.exec.fun.string.TrimFun;
import io.dingodb.exec.fun.string.UpperFun;
import io.dingodb.exec.fun.time.CurrentDateFun;
import io.dingodb.exec.fun.time.CurrentTimeFun;
import io.dingodb.exec.fun.time.CurrentTimestampFun;
import io.dingodb.exec.fun.time.DateDiffFun;
import io.dingodb.exec.fun.time.DateFormatFun;
import io.dingodb.exec.fun.time.FromUnixTimeFun;
import io.dingodb.exec.fun.time.TimeFormatFun;
import io.dingodb.exec.fun.time.TimestampFormatFun;
import io.dingodb.exec.fun.time.UnixTimestampEvaluatorFactory;
import io.dingodb.exec.fun.time.UnixTimestampEvaluators;
import io.dingodb.expr.parser.DefaultFunFactory;

public class DingoFunFactory extends DefaultFunFactory {
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
        registerUdf(CeilFun.NAME, CeilFun::new);
        registerUdf(FloorFun.NAME, FloorFun::new);
        registerUdf(FormatFun.NAME, FormatFun::new);
        registerUdf(ModFun.NAME, ModFun::new);
        registerUdf(PowFun.NAME, PowFun::new);
        registerUdf(RoundFun.NAME, RoundFun::new);
        // special
        funSuppliers.put(ArrayConstructorOp.NAME, ArrayConstructorOp::fun);
        funSuppliers.put(ListConstructorOp.NAME, ListConstructorOp::fun);
        funSuppliers.put(MapConstructorOp.NAME, MapConstructorOp::fun);
        funSuppliers.put(CastListItemsOp.NAME, CastListItemsOp::fun);
        funSuppliers.put(CaseOp.NAME, CaseOp::fun);
        registerUdf(RtSliceFun.NAME, RtSliceFun::new);
        // string
        registerUdf(CharLengthFun.NAME, CharLengthFun::new);
        registerUdf(ConcatFun.NAME, ConcatFun::new);
        registerUdf(LeftFun.NAME, LeftFun::new);
        registerUdf(LocateFun.NAME, LocateFun::new);
        registerUdf(LowerFun.NAME, LowerFun::new);
        registerUdf(LTrimFun.NAME, LTrimFun::new);
        registerUdf(MidFun.NAME, MidFun::new);
        registerUdf(RepeatFun.NAME, RepeatFun::new);
        registerUdf(ReplaceFun.NAME, ReplaceFun::new);
        registerUdf(ReverseFun.NAME, ReverseFun::new);
        registerUdf(RightFun.NAME, RightFun::new);
        registerUdf(RTrimFun.NAME, RTrimFun::new);
        registerUdf(SubstringFun.NAME, SubstringFun::new);
        registerUdf(TrimFun.NAME, TrimFun::new);
        registerUdf(UpperFun.NAME, UpperFun::new);
        // time
        registerUdf(CurrentDateFun.NAME, CurrentDateFun::new);
        registerUdf(CurrentTimeFun.NAME, CurrentTimeFun::new);
        registerUdf(CurrentTimestampFun.NAME, CurrentTimestampFun::new);
        registerUdf(FromUnixTimeFun.NAME, FromUnixTimeFun::new);
        registerEvaluator(UnixTimestampEvaluators.NAME, UnixTimestampEvaluatorFactory.INSTANCE);
        registerUdf(DateDiffFun.NAME, DateDiffFun::new);
        registerUdf(DateFormatFun.NAME, DateFormatFun::new);
        registerUdf(TimeFormatFun.NAME, TimeFormatFun::new);
        registerUdf(TimestampFormatFun.NAME, TimestampFormatFun::new);
    }
}
