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

package io.dingodb.net.netty;

public class Versions {

    public static final byte[] MAGIC_CODE = new byte[]{'D', 'I', 'N', 'G', 'O'};

    public static final byte VERSION_1 = 0x01;

    public static byte currentVersion() {
        return VERSION_1;
    }

    public static boolean checkCode(byte[] bytes, int start) {
        try {
            for (int i = 0; i < MAGIC_CODE.length; i++) {
                if (MAGIC_CODE[i] != bytes[i + start]) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

}

