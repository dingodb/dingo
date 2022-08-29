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

package io.dingodb.mpu.protocol;


import java.nio.ByteBuffer;

public class TagClock {

    public final byte tag;
    public final long clock;

    public TagClock(byte tag, long clock) {
        this.tag = tag;
        this.clock = clock;
    }

    public byte[] encode() {
        return ByteBuffer.allocate(9).put(tag).putLong(clock).array();
    }

    public static TagClock decode(byte[] content) {
        ByteBuffer buffer = ByteBuffer.wrap(content);
        return new TagClock(buffer.get(), buffer.getLong());
    }

}
