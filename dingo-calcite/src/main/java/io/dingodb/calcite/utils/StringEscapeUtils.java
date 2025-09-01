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

package io.dingodb.calcite.utils;

import org.apache.commons.lang3.text.translate.AggregateTranslator;
import org.apache.commons.lang3.text.translate.CharSequenceTranslator;
import org.apache.commons.lang3.text.translate.LookupTranslator;

public class StringEscapeUtils {
    static String[][] CHARS_ESCAPE = new String[][]{{"\n", "\\n"}, {"\t", "\\t"}, {"\r", "\\r"}};

    static String[][] CHARS_UNESCAPE;

    static {
        CHARS_UNESCAPE = invert(CHARS_ESCAPE);
    }

    public static String[][] invert(String[][] array) {
        String[][] newarray = new String[array.length][2];

        for (int i = 0; i < array.length; ++i) {
            newarray[i][0] = array[i][1];
            newarray[i][1] = array[i][0];
        }

        return newarray;
    }

    public static final CharSequenceTranslator UNESCAPE_DINGO =
        new AggregateTranslator(new CharSequenceTranslator[]{
            new LookupTranslator(CHARS_UNESCAPE),
            new LookupTranslator(new String[][]{{"\\\\", "\\"}, {"\\\"", "\""}, {"\\f", "f"}, {"\\b", ""}})});

    public static String unescape(String str) {
        return UNESCAPE_DINGO.translate(str);
    }
}
