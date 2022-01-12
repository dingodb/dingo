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

package io.dingodb.dingokv.util;

public class MetadataKeyHelper {

    public static char generatePrefix(long globalId) {
        char[] wordChar = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
                           'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'};
        char[] numChar = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9'};
        if (globalId % 2 == 0) {
            return numChar[(int) (globalId % numChar.length)];
        } else {
            return wordChar[(int) (globalId % wordChar.length)];
        }
    }

    private MetadataKeyHelper() {
    }
}
