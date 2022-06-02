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

import io.dingodb.net.Message;

import static io.dingodb.net.Message.API_OK;


public class Constant {
    public static final byte[] EMPTY_BYTES = new byte[0];
    public static final Object[] API_EMPTY_ARGS = new Object[0];

    public static final Message API_VOID = new Message(API_OK, EMPTY_BYTES);

}
