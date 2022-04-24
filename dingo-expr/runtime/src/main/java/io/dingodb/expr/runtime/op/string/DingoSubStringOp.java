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
public class DingoSubStringOp extends RtStringConversionOp {
    private static final long serialVersionUID = 9195218471853466015L;

    /**
     * Create an DingoSubStringOp. RtTrimOp trim the leading and trailing blanks of a String by {@code String::trim}.
     *
     * @param paras the parameters of the op
     */
    public DingoSubStringOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Nonnull
    @Override
    protected Object fun(@Nonnull Object[] values) {
        String inputStr = ((String) values[0]);

        BigDecimal decimal = new BigDecimal(values[1].toString());
        Integer startIndex = decimal.setScale(0, BigDecimal.ROUND_HALF_UP).intValue();

        decimal = new BigDecimal(values[2].toString());
        Integer cnt = decimal.setScale(0, BigDecimal.ROUND_HALF_UP).intValue();

        return subStr(inputStr, startIndex, cnt);
    }

    public static String subStr(final String inputStr, final int index, final int cnt) {
        if (inputStr == null || inputStr.isEmpty()) {
            return "";
        }

        if (cnt < 0) {
            return "";
        }

        int startIndex = index;
        if (startIndex < 0) {
            startIndex = startIndex + inputStr.length() + 1;
        }
        startIndex = startIndex - 1;

        if (startIndex + cnt > inputStr.length()) {
            if (startIndex == inputStr.length()) {
                startIndex = startIndex + 1;
            }
            return inputStr.substring(startIndex, inputStr.length());
        }

        return inputStr.substring(startIndex, startIndex + cnt);
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoSubStringOp::new;
        }

        @Override
        public List<String> name() {
            return Arrays.asList("substring");
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DingoSubStringOp.class.getMethod("subStr", String.class, int.class, int.class));
                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e.toString(), e);
                throw new RuntimeException(e);
            }
        }
    }
}
