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

package io.dingodb.calcite.func;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class DingoFunc {
    private DingoFunc() {
    }

    public static final Map<String, String> DINGO_FUNC_LIST = new HashMap<String, String>();

    static {
        DINGO_FUNC_LIST.put("ltrim".toUpperCase(), "trimLeft");
        DINGO_FUNC_LIST.put("rtrim".toUpperCase(), "trimRight");
        DINGO_FUNC_LIST.put("lcase".toUpperCase(), "toLowCase");
        DINGO_FUNC_LIST.put("ucase".toUpperCase(), "toUpCase");
        DINGO_FUNC_LIST.put("left".toUpperCase(), "leftString");
        DINGO_FUNC_LIST.put("right".toUpperCase(), "rightString");
        DINGO_FUNC_LIST.put("reverse".toUpperCase(), "reverseString");
        DINGO_FUNC_LIST.put("repeat".toUpperCase(), "repeatString");
        DINGO_FUNC_LIST.put("mid".toUpperCase(), "midString");
        DINGO_FUNC_LIST.put("locate".toUpperCase(), "locateString");
        DINGO_FUNC_LIST.put("concat".toUpperCase(), "concatString");
    }

    public static String trimLeft(String inputStr) {
        if (inputStr == null || inputStr.equals("")) {
            return inputStr;
        } else {
            return inputStr.replaceAll("^[　 ]+", "");
        }
    }

    public static String trimRight(String str) {
        if (str == null || str.equals("")) {
            return str;
        } else {
            return str.replaceAll("[　 ]+$", "");
        }
    }

    public static String toLowCase(String str) {
        if (str == null || str.equals("")) {
            return str;
        } else {
            return str.toLowerCase();
        }
    }

    public static String toUpCase(String str) {
        if (str == null || str.equals("")) {
            return str;
        } else {
            return str.toUpperCase();
        }
    }

    public static String leftString(String str, int cnt) {
        if (str == null || str.equals("") || cnt > str.length()) {
            return str;
        } else {
            return str.substring(0, cnt);
        }
    }

    public static String rightString(String str, int cnt) {
        if (str == null || str.equals("") || cnt > str.length()) {
            return str;
        } else {
            return str.substring(str.length() - cnt, str.length());
        }
    }

    public static String reverseString(String inputStr) {
        if (inputStr == null || inputStr.equals("")) {
            return inputStr;
        } else {
            return new StringBuilder(inputStr).reverse().toString();
        }
    }

    public static String repeatString(String inputStr, int times) {
        if (inputStr == null || inputStr.equals("")) {
            return inputStr;
        } else {
            return String.join("", Collections.nCopies(times, inputStr));
        }
    }

    public static String midString(String inputStr, int startIndex, int cnt) {
        if (inputStr == null || inputStr.equals("") || startIndex + cnt > inputStr.length()) {
            return inputStr;
        } else {
            return inputStr.substring(startIndex, startIndex + cnt);
        }
    }

    public static String concatString(String[] inputs) {
        String result = "";
        for (String input: inputs) {
            result += input;
        }
        return result;
    }

    public static int locateString(String subString, String inputStr) {
        if (inputStr == null || inputStr.equals("")) {
            return -1;
        } else {
            return inputStr.indexOf(subString);
        }
    }

}
