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

package io.dingodb.mpu;

import io.dingodb.net.NetService;
import io.dingodb.net.api.ApiRegistry;

public class Constant {

    private Constant() {
    }

    // net service
    public static final NetService NET = NetService.getDefault();
    public static final ApiRegistry API = ApiRegistry.getDefault();

    // inner tag
    public static final byte T_INSTRUCTION = 1;
    public static final byte T_SYNC = 2;
    public static final byte T_EXECUTED_CLOCK = 3;
    public static final byte T_EXECUTE_INSTRUCTION = 4;

    // storage key
    public static final byte[] CLOCK_K = "CLOCK".getBytes();

    // storage namespace
    public static final byte[] CF_DEFAULT = "default".getBytes();
    public static final byte[] CF_META = "META".getBytes();
    public static final byte[] CF_DATA = "DATA".getBytes();
    public static final byte[] CF_INSTRUCTION = "INSTRUCTION".getBytes();
}
