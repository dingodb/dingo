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

package io.dingodb.raft.entity.codec.v1;

import io.dingodb.raft.entity.EnumOutter;
import io.dingodb.raft.entity.LogEntry;
import io.dingodb.raft.entity.LogId;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.entity.codec.LogEntryDecoder;
import io.dingodb.raft.util.AsciiStringUtil;
import io.dingodb.raft.util.Bits;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
@Deprecated
public final class V1Decoder implements LogEntryDecoder {
    private V1Decoder() {
    }

    public static final V1Decoder INSTANCE = new V1Decoder();

    @Override
    public LogEntry decode(final byte[] content) {
        if (content == null || content.length == 0) {
            return null;
        }
        if (content[0] != LogEntryV1CodecFactory.MAGIC) {
            // Corrupted log
            return null;
        }
        LogEntry log = new LogEntry();
        decode(log, content);

        return log;
    }

    public void decode(final LogEntry log, final byte[] content) {
        // 1-5 type
        final int iType = Bits.getInt(content, 1);
        log.setType(EnumOutter.EntryType.forNumber(iType));
        // 5-13 index
        // 13-21 term
        final long index = Bits.getLong(content, 5);
        final long term = Bits.getLong(content, 13);
        log.setId(new LogId(index, term));
        // 21-25 peer count
        int peerCount = Bits.getInt(content, 21);
        // peers
        int pos = 25;
        if (peerCount > 0) {
            List<PeerId> peers = new ArrayList<>(peerCount);
            while (peerCount-- > 0) {
                final short len = Bits.getShort(content, pos);
                final byte[] bs = new byte[len];
                System.arraycopy(content, pos + 2, bs, 0, len);
                // peer len (short in 2 bytes)
                // peer str
                pos += 2 + len;
                final PeerId peer = new PeerId();
                peer.parse(AsciiStringUtil.unsafeDecode(bs));
                peers.add(peer);
            }
            log.setPeers(peers);
        }
        // old peers
        int oldPeerCount = Bits.getInt(content, pos);
        pos += 4;
        if (oldPeerCount > 0) {
            List<PeerId> oldPeers = new ArrayList<>(oldPeerCount);
            while (oldPeerCount-- > 0) {
                final short len = Bits.getShort(content, pos);
                final byte[] bs = new byte[len];
                System.arraycopy(content, pos + 2, bs, 0, len);
                // peer len (short in 2 bytes)
                // peer str
                pos += 2 + len;
                final PeerId peer = new PeerId();
                peer.parse(AsciiStringUtil.unsafeDecode(bs));
                oldPeers.add(peer);
            }
            log.setOldPeers(oldPeers);
        }

        // data
        if (content.length > pos) {
            final int len = content.length - pos;
            ByteBuffer data = ByteBuffer.allocate(len);
            data.put(content, pos, len);
            data.flip();
            log.setData(data);
        }
    }
}
