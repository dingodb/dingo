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

package io.dingodb.raft.entity.codec;

import io.dingodb.raft.entity.LogEntry;
import io.dingodb.raft.entity.codec.v1.V1Decoder;
import io.dingodb.raft.entity.codec.v2.LogEntryV2CodecFactory;
import io.dingodb.raft.entity.codec.v2.V2Decoder;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class AutoDetectDecoder implements LogEntryDecoder {
    private AutoDetectDecoder() {

    }

    public static final AutoDetectDecoder INSTANCE = new AutoDetectDecoder();

    @Override
    public LogEntry decode(final byte[] bs) {
        if (bs == null || bs.length < 1) {
            return null;
        }

        if (bs[0] == LogEntryV2CodecFactory.MAGIC_BYTES[0]) {
            return V2Decoder.INSTANCE.decode(bs);
        } else {
            return V1Decoder.INSTANCE.decode(bs);
        }
    }

}
