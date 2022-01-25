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

package io.dingodb.raft.entity.codec.v2;

import io.dingodb.raft.entity.codec.AutoDetectDecoder;
import io.dingodb.raft.entity.codec.LogEntryCodecFactory;
import io.dingodb.raft.entity.codec.LogEntryDecoder;
import io.dingodb.raft.entity.codec.LogEntryEncoder;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class LogEntryV2CodecFactory implements LogEntryCodecFactory {
    private static final LogEntryV2CodecFactory INSTANCE = new LogEntryV2CodecFactory();

    public static LogEntryV2CodecFactory getInstance() {
        return INSTANCE;
    }

    // BB-8 and R2D2 are good friends.
    public static final byte[] MAGIC_BYTES = new byte[] { (byte) 0xBB, (byte) 0xD2 };
    // Codec version
    public static final byte VERSION = 1;

    public static final byte[] RESERVED = new byte[3];

    public static final int HEADER_SIZE = MAGIC_BYTES.length + 1 + RESERVED.length;

    @Override
    public LogEntryEncoder encoder() {
        return V2Encoder.INSTANCE;
    }

    @Override
    public LogEntryDecoder decoder() {
        return AutoDetectDecoder.INSTANCE;
    }

    private LogEntryV2CodecFactory() {
    }
}
