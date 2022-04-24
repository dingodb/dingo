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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nonnull;

@Slf4j
public class DingoStringTrimOp extends RtStringConversionOp {
    private static final long serialVersionUID = -4332118553191666565L;

    /**
     * Create an DingoStringTrimOp.
     * DingoStringTrimOp trim the leading and trailing blanks of a String by {@code String::trim}.
     *
     * @param paras the parameters of the op
     */
    public DingoStringTrimOp(@Nonnull RtExpr[] paras) {
        super(paras);
    }

    @Nonnull
    @Override
    protected Object fun(@Nonnull Object[] values) {
        if (values.length != 3) {
            return ((String) values[0]).trim();
        }

        String opType = (String)values[0];
        String trimStr = (String)values[1];
        String inputStr = (String)values[2];

        return trimStr(opType, trimStr, inputStr);
    }

    /**
     * return the last index where trimStr not substr of inputStr.
     * Input: 123123123, Trim:123, return: 9
     * Input: aaaa, Trim:a, return: 4
     *
     * @param inputStr input string
     * @param trimStr trim string
     * @return index
     */
    private static int getLastIndexFromLeft(final String inputStr, final String trimStr) {
        int result = 0;
        int index = 0;
        boolean isFound = false;

        while (inputStr.indexOf(trimStr, index) == index && index <= inputStr.length() - 1) {
            result = index;
            index += trimStr.length();
            isFound = true;
        }

        if (isFound) {
            return result + trimStr.length();
        } else {
            return 0;
        }
    }


    private static int getLastIndexFromRight(final String inputStr, final String trimStr) {
        if (inputStr.length() < trimStr.length()) {
            return inputStr.length() - 1;
        }

        int index = inputStr.length() - trimStr.length();
        int result = inputStr.length() - 1;
        boolean isFound = false;
        while ((inputStr.lastIndexOf(trimStr, index)) == index && index >= 0) {
            result = index;
            index -= trimStr.length();
            isFound = true;
        }

        if (isFound) {
            return result;
        } else {
            return inputStr.length();
        }
    }

    public static String trimStr(final String opType, final String trimStr, final String inputStr) {
        int startIndex = 0;
        int endIndex = inputStr.length();
        switch (opType) {
            case "BOTH": {
                startIndex = getLastIndexFromLeft(inputStr, trimStr);
                endIndex = getLastIndexFromRight(inputStr, trimStr);
                break;
            }
            case "LEADING": {
                startIndex = getLastIndexFromLeft(inputStr, trimStr);
                break;
            }
            case "TRAILING": {
                endIndex = getLastIndexFromRight(inputStr, trimStr);
                break;
            }
            default:
                break;
        }

        if (startIndex >= endIndex) {
            return "";
        } else {
            return inputStr.substring(startIndex, endIndex);
        }
    }

    @AutoService(DingoFuncProvider.class)
    public static class Provider implements DingoFuncProvider {

        public Function<RtExpr[], RtOp> supplier() {
            return DingoStringTrimOp::new;
        }

        @Override
        public List<String> name() {
            return Arrays.asList("trim");
        }

        @Override
        public List<Method> methods() {
            try {
                List<Method> methods = new ArrayList<>();
                methods.add(DingoStringTrimOp.class.getMethod("trimStr", String.class, String.class,
                    String.class));
                return methods;
            } catch (NoSuchMethodException e) {
                log.error("Method:{} NoSuchMethodException:{}", this.name(), e.toString(), e);
                throw new RuntimeException(e);
            }
        }
    }
}
