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

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class MetaListenEvent {

    public enum Event {
        CREATE_SCHEMA,
        CREATE_TABLE,
        UPDATE_SCHEMA,
        UPDATE_TABLE,
        DELETE_SCHEMA,
        DELETE_TABLE
    }

    public final Event event;
    public final Object meta;

    public <M> M meta() {
        return (M) meta;
    }

}
