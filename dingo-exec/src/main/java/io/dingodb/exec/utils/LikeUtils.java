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

package io.dingodb.exec.utils;

import java.util.regex.Pattern;

public class LikeUtils {

    public static Pattern getPattern(String patternStr, boolean binary) {
        StringBuilder buf = new StringBuilder();
        buf.append("^");
        char[] charArray = patternStr.toCharArray();
        int backslashIndex = -2;

        for (int i = 0; i < charArray.length; i++) {
            char c = charArray[i];
            if (backslashIndex == i - 1) {
                buf.append('[').append(c).append(']');
                continue;
            }
            switch (c) {
                case '\\':
                    backslashIndex = i;
                    break;
                case '%':
                    buf.append(".*");
                    break;
                case '_':
                    buf.append(".");
                    break;
                default:
                    buf.append(c);
                    break;
            }
        }
        buf.append("$");

        if (binary) {
            return Pattern.compile(buf.toString());
        } else {
            return Pattern.compile(buf.toString(), Pattern.CASE_INSENSITIVE);
        }
    }
}
