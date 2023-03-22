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

package io.dingodb.common.mysql.constant;

public class ServerConstant {
    public static final byte[] SEED = new byte[]{
        80, 110, 92, 95, 73, 48, 84, 53
    };


    public static final byte[] REST_OF_SCRAMBLE = new byte[]{
        74, 87, 87, 45, 120, 58, 84, 122, 41, 91, 110, 58
    };

    public static final byte[] unused = new byte[]{
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    };

}
