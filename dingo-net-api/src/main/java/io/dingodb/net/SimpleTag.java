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

import java.nio.charset.StandardCharsets;

@Getter
@Builder
@EqualsAndHashCode
@AllArgsConstructor
public class SimpleTag implements Tag {
    public static final Tag DEFAULT_TAG = SimpleTag.builder()
        .tag(new byte[0])
        .build();
    public static final Tag TASK_TAG = SimpleTag.builder()
        .tag("DINGO_TASK".getBytes(StandardCharsets.UTF_8))
        .build();
    public static final Tag CTRL_TAG = SimpleTag.builder()
        .tag("DINGO_CTRL".getBytes(StandardCharsets.UTF_8))
        .build();

    private byte[] tag;

    @Override
    public byte[] toBytes() {
        return tag;
    }

    @Override
    public SimpleTag load(byte[] bytes) {
        this.tag = bytes;
        return this;
    }

    @Override
    public int flag() {
        return 0;
    }

    @Override
    public String toString() {
        return new String(tag, StandardCharsets.UTF_8);
    }
}
