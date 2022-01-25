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

package io.dingodb.raft.storage.log;

import com.google.protobuf.ZeroByteStringHelper;
import io.dingodb.raft.storage.io.ProtoBufFile;
import io.dingodb.raft.entity.LocalFileMetaOutter.LocalFileMeta;
import io.dingodb.raft.util.AsciiStringUtil;
import io.dingodb.raft.util.Bits;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class CheckpointFile {
    /**
     * Checkpoint metadata info.
     *
     * @author boyan(boyan@antfin.com)
     */
    public static final class Checkpoint {
        // Segment file name
        public String segFilename;
        // Segment file current commit position.
        public int committedPos;

        public Checkpoint(final String segFilename, final int committedPos) {
            super();
            this.segFilename = segFilename;
            this.committedPos = committedPos;
        }

        /**
         * commitPos (4 bytes) + path(4 byte len + string bytes)
         */
        byte[] encode() {
            byte[] ps = AsciiStringUtil.unsafeEncode(this.segFilename);
            byte[] bs = new byte[8 + ps.length];
            Bits.putInt(bs, 0, this.committedPos);
            Bits.putInt(bs, 4, ps.length);
            System.arraycopy(ps, 0, bs, 8, ps.length);
            return bs;
        }

        boolean decode(final byte[] bs) {
            if (bs.length < 8) {
                return false;
            }
            this.committedPos = Bits.getInt(bs, 0);
            int len = Bits.getInt(bs, 4);
            this.segFilename = AsciiStringUtil.unsafeDecode(bs, 8, len);
            return this.committedPos >= 0 && !this.segFilename.isEmpty();
        }

        @Override
        public String toString() {
            return "Checkpoint [segFilename=" + this.segFilename + ", committedPos=" + this.committedPos + "]";
        }
    }

    public void destroy() {
        FileUtils.deleteQuietly(new File(this.path));
    }

    public String getPath() {
        return this.path;
    }

    private final String path;

    public CheckpointFile(final String path) {
        super();
        this.path = path;
    }

    public synchronized boolean save(final Checkpoint checkpoint) throws IOException {
        final ProtoBufFile file = new ProtoBufFile(this.path);
        final byte[] data = checkpoint.encode();

        final LocalFileMeta meta = LocalFileMeta.newBuilder() //
            .setUserMeta(ZeroByteStringHelper.wrap(data)) //
            .build();

        return file.save(meta, true);
    }

    public Checkpoint load() throws IOException {
        final ProtoBufFile file = new ProtoBufFile(this.path);
        final LocalFileMeta meta = file.load();
        if (meta != null) {
            final byte[] data = meta.getUserMeta().toByteArray();
            Checkpoint checkpoint = new Checkpoint(null, -1);
            if (checkpoint.decode(data)) {
                return checkpoint;
            }
        }
        return null;
    }
}
