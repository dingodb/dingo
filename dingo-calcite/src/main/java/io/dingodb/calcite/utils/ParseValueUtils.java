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

import io.dingodb.common.util.ByteUtils;

public final class ParseValueUtils {

    private ParseValueUtils() {
    }

    public static int positiveInteger(String source, String field) {
        try {
            int value = Integer.parseInt(source);
            if (value > 0) {
                return value;
            } else {
                throw new RuntimeException();
            }
        } catch (Exception ignore) {
            throw new IllegalArgumentException("The " + field + " need a positive integer, but [" + source + "].");
        }
    }

    public static byte[] getSpecialBytes(String image) {
        return image.replace("'", "").toLowerCase().getBytes();
    }

    public static byte[] getSpecialHexBytes(String image) {
        return ByteUtils.hexStringToByteArray(image.replace("'", "").toLowerCase().substring(1));
    }

}
