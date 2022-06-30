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

package io.dingodb.sdk.common;

import io.dingodb.sdk.common.Value;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@Data
@AllArgsConstructor
public final class Key {

    /**
     * Optional database for the key.
     */
    private String database;

    /**
     * Required table name.
     */
    private final String table;

    /**
     * Original user key. This key is immediately converted to a hash digest.
     * This key is not used or returned by the server by default. If the user key needs
     * to persist on the server, use one of the following methods:
     * <ul>
     * <li>Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
     * and retrieved on multi-record scans and queries.</li>
     * <li>Explicitly store and retrieve the key in a bin.</li>
     * </ul>
     */
    public final List<Value> userKey;
}
