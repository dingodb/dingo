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

import io.dingodb.exec.fun.mysql.VersionFun;
import io.dingodb.exec.fun.special.ThrowFun;
import io.dingodb.exec.fun.vector.VectorCosineDistanceFun;
import io.dingodb.exec.fun.vector.VectorDistanceFun;
import io.dingodb.exec.fun.vector.VectorIPDistanceFun;
import io.dingodb.exec.fun.vector.VectorImageFun;
import io.dingodb.exec.fun.vector.VectorL2DistanceFun;
import io.dingodb.exec.fun.vector.VectorTextFun;
import io.dingodb.expr.parser.DefaultFunFactory;
import io.dingodb.expr.runtime.ExprConfig;
import io.dingodb.expr.runtime.expr.Exprs;

public class DingoFunFactory extends DefaultFunFactory {
    public static final String SUBSTRING = "SUBSTRING";

    private static DingoFunFactory instance;

    private DingoFunFactory() {
        super(ExprConfig.ADVANCED);
        registerBinaryFun(SUBSTRING, Exprs.MID2);
        registerTertiaryFun(SUBSTRING, Exprs.MID3);
        registerBinaryFun(PowFunFactory.NAME, PowFunFactory.INSTANCE);
        registerNullaryFun(ThrowFun.NAME, ThrowFun.INSTANCE);

        registerBinaryFun(AutoIncrementFun.NAME, AutoIncrementFun.INSTANCE);
        registerTertiaryFun(VectorImageFun.NAME, VectorImageFun.INSTANCE);
        registerBinaryFun(VectorTextFun.NAME, VectorTextFun.INSTANCE);
        registerBinaryFun(VectorL2DistanceFun.NAME, VectorL2DistanceFun.INSTANCE);
        registerBinaryFun(VectorIPDistanceFun.NAME, VectorIPDistanceFun.INSTANCE);
        registerBinaryFun(VectorCosineDistanceFun.NAME, VectorCosineDistanceFun.INSTANCE);
        registerBinaryFun(VectorDistanceFun.NAME, VectorDistanceFun.INSTANCE);
        registerNullaryFun(VersionFun.NAME, VersionFun.INSTANCE);
    }

    public static synchronized DingoFunFactory getInstance() {
        if (instance == null) {
            instance = new DingoFunFactory();
        }
        return instance;
    }
}
