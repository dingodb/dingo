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

package io.dingodb.expr.runtime.op.string;

import com.google.auto.service.AutoService;
import io.dingodb.expr.runtime.RtExpr;
import io.dingodb.expr.runtime.op.RtOp;
import io.dingodb.func.DingoFuncProvider;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;

public class DingoStringRightOp extends RtStringConversionOp {
    private static final long serialVersionUID = 1043463700086782931L;

    /**
     * Create an DingoStringRightOp. DingoStringRightOp extract right sub string.
     *
     * @param paras the parameters of the op
     */
    public DingoStringRightOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Nonnull
    @Override
    protected Object fun(@Nonnull Object[] values) {
        String inputStr = (String) values[0];
        Integer cnt = new BigDecimal(String.valueOf(values[1]))
            .setScale(0, BigDecimal.ROUND_HALF_UP).intValue();

        if (cnt < 0) {
            return "";
        }

        if (inputStr.length() > cnt) {
            return inputStr.substring(inputStr.length() - cnt, inputStr.length());
        }
        return inputStr;
    }

    public static String rightString(final String str, int cnt) {
        if (str == null || str.equals("") || cnt > str.length()) {
            return str;
        } else {
            return str.substring(str.length() - cnt, str.length());
        }
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoStringRightOp::new;
        }

        @Override
        public String name() {
            return "right";
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DingoStringRightOp.class.getMethod("rightString", String.class, int.class));
                return methods;
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
