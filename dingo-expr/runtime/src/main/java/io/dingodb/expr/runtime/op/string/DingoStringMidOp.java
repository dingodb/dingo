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
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;

@Slf4j
public class DingoStringMidOp extends RtStringConversionOp {
    private static final long serialVersionUID = 8185618697441684894L;

    /**
     * Create an DingoStringMidOp. get the mid of string.
     *
     * @param paras the parameters of the op
     */
    public DingoStringMidOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Nonnull
    @Override
    protected Object fun(@Nonnull Object[] values) {
        String inputStr = ((String)values[0]);

        BigDecimal decimal = new BigDecimal(values[1].toString());
        Integer startIndex = decimal.setScale(0, BigDecimal.ROUND_HALF_UP).intValue();

        if (values.length == 2) {
            return midString(inputStr, startIndex);
        }

        decimal = new BigDecimal(values[2].toString());
        Integer cnt = decimal.setScale(0, BigDecimal.ROUND_HALF_UP).intValue();
        return midString(inputStr, startIndex, cnt);
    }

    public static String midString(final String inputStr, int startIndex) {
        if (inputStr == null || inputStr.length() == 0) {
            return "";
        }

        if (startIndex < 0) {
            startIndex = startIndex + inputStr.length() + 1;
        }

        if (startIndex - 1 == inputStr.length()) {
            startIndex = startIndex + 2;
        }

        return inputStr.substring(startIndex - 1);
    }

    public static String midString(final String inputStr, int startIndex, int cnt) {
        if (inputStr == null || inputStr.length() == 0 || cnt < 0) {
            return "";
        }

        if (startIndex < 0) {
            startIndex = startIndex + inputStr.length() + 1;
        }

        int endIndex = (startIndex + cnt - 1 > inputStr.length() ? inputStr.length() : startIndex + cnt - 1);
        if (startIndex - 1 == inputStr.length()) {
            startIndex = startIndex + 2;
        }

        return inputStr.substring(startIndex - 1, endIndex);
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoStringMidOp::new;
        }

        @Override
        public List<String> name() {
            return Arrays.asList("mid");
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DingoStringMidOp.class.getMethod("midString", String.class, int.class));
                methods.add(DingoStringMidOp.class.getMethod("midString", String.class, int.class, int.class));
                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e.toString(), e);
                throw new RuntimeException(e);
            }
        }
    }
}
