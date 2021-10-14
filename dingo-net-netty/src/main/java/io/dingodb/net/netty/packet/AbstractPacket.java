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

package io.dingodb.net.netty.packet;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.experimental.Delegate;
import lombok.experimental.FieldNameConstants;

@Accessors(fluent = true)
@Getter
@ToString
@FieldNameConstants
@AllArgsConstructor
@RequiredArgsConstructor
public abstract class AbstractPacket<T> implements Packet<T> {

    @NonNull
    @Delegate
    protected final Header header;
    protected T content;

    @Override
    public Header header() {
        return header;
    }

    @Override
    public T content() {
        return content;
    }
}
