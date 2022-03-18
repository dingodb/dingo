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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public final class DingoFunc {
    private DingoFunc() {
    }

    public static final Map<String, String> DINGO_FUNC_LIST = new HashMap<String, String>();

    static {
        DINGO_FUNC_LIST.put("ltrim".toUpperCase(), "trimLeft");
        DINGO_FUNC_LIST.put("rtrim".toUpperCase(), "trimRight");
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
}
