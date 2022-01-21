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

package io.dingodb.net;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Builder
@Getter
@EqualsAndHashCode
@AllArgsConstructor
public class SimpleMessage implements Message {

    public static final SimpleMessage EMPTY = new SimpleMessage(SimpleTag.DEFAULT_TAG, new byte[0]);

    private Tag tag;
    private byte[] content;

    @Override
    public Tag tag() {
        return tag;
    }

    @Override
    public byte[] toBytes() {
        return content;
    }

    @Override
    public SimpleMessage load(byte[] bytes) {
        this.content = bytes;
        return this;
    }
}
