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

public class NumberUtils {
    private NumberUtils() {
    }

    public static int posMod(int dividend, int divisor) {
        if (divisor < 0) {
            divisor = -divisor;
        }
        int index = dividend % divisor;
        return index >= 0 ? index : index + divisor;
    }

    public static long posMod(long dividend, long divisor) {
        if (divisor < 0) {
            divisor = -divisor;
        }
        long index = dividend % divisor;
        return index >= 0 ? index : index + divisor;
    }
}
