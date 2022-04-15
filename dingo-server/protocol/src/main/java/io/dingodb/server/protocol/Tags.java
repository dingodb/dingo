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

package io.dingodb.server.protocol;

import io.dingodb.net.SimpleTag;
import io.dingodb.net.Tag;

import java.nio.charset.StandardCharsets;

public class Tags {

    public static final Tag RAFT_SERVICE = new SimpleTag("RAFT_SERVICE".getBytes(StandardCharsets.UTF_8));
    public static final Tag LISTEN_RAFT_LEADER = new SimpleTag("RAFT_SERVICE".getBytes(StandardCharsets.UTF_8));
    public static final Tag META_SERVICE = new SimpleTag("META_SERVICE".getBytes(StandardCharsets.UTF_8));

    public static final Tag SERVICE_STATS = new SimpleTag("SERVICE_STATS".getBytes(StandardCharsets.UTF_8));

    private Tags() {
    }
}
