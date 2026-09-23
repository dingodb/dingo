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

import io.dingodb.exec.fun.mysql.DatabaseFun;
import io.dingodb.exec.fun.mysql.HexFun;
import io.dingodb.exec.fun.mysql.JsonExtractFun;
import io.dingodb.exec.fun.mysql.SchemaFun;
import io.dingodb.exec.fun.mysql.ScopeVarFun;
import io.dingodb.exec.fun.mysql.UnHexFun;
import io.dingodb.exec.fun.mysql.UserDefVarFun;
import io.dingodb.exec.fun.mysql.UserFun;
import io.dingodb.exec.fun.mysql.VersionFun;
import io.dingodb.exec.fun.mysql.ConnectionIdFun;
import io.dingodb.exec.fun.mysql.ConvertTzFun;
import io.dingodb.exec.fun.mysql.QuoteFun;
import io.dingodb.exec.fun.mysql.CharCharsetFun;
import io.dingodb.exec.fun.mysql.CharBinaryFun;
import io.dingodb.exec.fun.mysql.CharFun;
import io.dingodb.exec.fun.mysql.ConvertCharsetFun;
import io.dingodb.exec.fun.mysql.ConvertBinaryFun;
import io.dingodb.exec.fun.mysql.LeastFun;
import io.dingodb.exec.fun.sequence.CurrValFun;
import io.dingodb.exec.fun.sequence.LastValFun;
import io.dingodb.exec.fun.sequence.NextValFun;
import io.dingodb.exec.fun.sequence.SetValFun;
import io.dingodb.exec.fun.special.ThrowFun;
import io.dingodb.exec.fun.vector.VectorCosineDistanceFun;
import io.dingodb.exec.fun.vector.VectorDistanceFun;
import io.dingodb.exec.fun.vector.VectorHammingDistanceFun;
import io.dingodb.exec.fun.vector.VectorIPDistanceFun;
import io.dingodb.exec.fun.vector.VectorImageFun;
import io.dingodb.exec.fun.vector.VectorL2DistanceFun;
import io.dingodb.exec.fun.vector.VectorTextFun;
import io.dingodb.expr.parser.DefaultFunFactory;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.expr.Exprs;
import io.dingodb.expr.runtime.op.string.InstrFun;
import io.dingodb.expr.runtime.op.time.CurrentTimestampFun;

public class DingoFunFactory extends DefaultFunFactory {
    public static final String SUBSTRING = "SUBSTRING";

    private static DingoFunFactory instance;

    private DingoFunFactory() {
        super(ExprConfig.ADVANCED);
        registerBinaryFun(SUBSTRING, Exprs.MID2);
        registerTertiaryFun(SUBSTRING, Exprs.MID3);
        registerTertiaryFun("SUBSTR", Exprs.MID3);
        registerBinaryFun(PowFunFactory.NAME, PowFunFactory.INSTANCE);
        registerNullaryFun(ThrowFun.NAME, ThrowFun.INSTANCE);
        registerNullaryFun("NOW", CurrentTimestampFun.INSTANCE);

        registerBinaryFun(AutoIncrementFun.NAME, AutoIncrementFun.INSTANCE);
        registerTertiaryFun(VectorImageFun.NAME, VectorImageFun.INSTANCE);
        registerBinaryFun(VectorTextFun.NAME, VectorTextFun.INSTANCE);
        registerBinaryFun(VectorL2DistanceFun.NAME, VectorL2DistanceFun.INSTANCE);
        registerBinaryFun(VectorIPDistanceFun.NAME, VectorIPDistanceFun.INSTANCE);
        registerBinaryFun(VectorCosineDistanceFun.NAME, VectorCosineDistanceFun.INSTANCE);
        registerBinaryFun(VectorHammingDistanceFun.NAME, VectorHammingDistanceFun.INSTANCE);
        registerBinaryFun(VectorDistanceFun.NAME, VectorDistanceFun.INSTANCE);
        registerNullaryFun(VersionFun.NAME, VersionFun.INSTANCE);
        registerVariadicFun(JsonExtractFun.NAME, JsonExtractFun.INSTANCE);
        registerBinaryFun(DatabaseFun.NAME, DatabaseFun.INSTANCE);
        registerBinaryFun(ScopeVarFun.NAME, ScopeVarFun.INSTANCE);
        registerBinaryFun(UserDefVarFun.NAME, UserDefVarFun.INSTANCE);
        registerBinaryFun(UserFun.NAME, UserFun.INSTANCE);
        registerBinaryFun(InstrFun.NAME, InstrFun.INSTANCE);
        registerBinaryFun(SchemaFun.NAME, SchemaFun.INSTANCE);
        registerBinaryFun(StrToDateFun.NAME, StrToDateFun.INSTANCE);
        registerUnaryFun(NextValFun.NAME, NextValFun.INSTANCE);
        registerUnaryFun(CurrValFun.NAME, CurrValFun.INSTANCE);
        registerUnaryFun(LastValFun.NAME, LastValFun.INSTANCE);
        registerBinaryFun(SetValFun.NAME, SetValFun.INSTANCE);
        registerUnaryFun(LengthFun.NAME, LengthFun.INSTANCE);
        registerBinaryFun(LengthFun.CHARSET_NAME, LengthFun.CHARSET_INSTANCE);
        registerTertiaryFun(IfFun.NAME, IfFun.INSTANCE);
        registerBinaryFun(DateAddFun.NAME, DateAddFun.INSTANCE);
        registerBinaryFun(DateSubFun.NAME, DateSubFun.INSTANCE);
        registerUnaryFun(DateFun.NAME, DateFun.INSTANCE);
        registerUnaryFun(ValuesFun.NAME, ValuesFun.INSTANCE);
        registerUnaryFun(UnHexFun.NAME, UnHexFun.INSTANCE);
        registerUnaryFun(HexFun.NAME, HexFun.INSTANCE);
        registerBinaryFun(HexFun.CHARSET_NAME, HexFun.CHARSET_INSTANCE);
        registerBinaryFun(DaySubFun.NAME, DaySubFun.INSTANCE);
        registerUnaryFun(GetDateFun.NAME, GetDateFun.INSTANCE);
        registerVariadicFun(ConcatWsFun.NAME, ConcatWsFun.INSTANCE);
        registerVariadicFun(CharFun.NAME, CharFun.INSTANCE);
        registerVariadicFun(CharCharsetFun.NAME, CharCharsetFun.INSTANCE);
        registerVariadicFun(CharBinaryFun.NAME, CharBinaryFun.INSTANCE);
        registerBinaryFun(ConvertCharsetFun.NAME, ConvertCharsetFun.INSTANCE);
        registerBinaryFun(ConvertBinaryFun.NAME, ConvertBinaryFun.INSTANCE);
        registerUnaryFun(QuoteFun.NAME, QuoteFun.INSTANCE);
        registerBinaryFun(ConnectionIdFun.NAME, ConnectionIdFun.INSTANCE);
        registerTertiaryFun(ConvertTzFun.NAME, ConvertTzFun.INSTANCE);
        registerBinaryFun(LeastFun.NAME, LeastFun.INSTANCE);
    }

    public static synchronized DingoFunFactory getInstance() {
        if (instance == null) {
            instance = new DingoFunFactory();
        }
        return instance;
    }
}
