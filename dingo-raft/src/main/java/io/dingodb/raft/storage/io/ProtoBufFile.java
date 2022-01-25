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

package io.dingodb.raft.storage.io;

import com.google.protobuf.Message;
import io.dingodb.raft.rpc.ProtobufMsgFactory;
import io.dingodb.raft.util.Bits;
import io.dingodb.raft.util.Utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class ProtoBufFile {
    static {
        ProtobufMsgFactory.load();
    }

    /** file path */
    private final String path;

    public ProtoBufFile(final String path) {
        this.path = path;
    }

    /**
     * Load a protobuf message from file.
     */
    public <T extends Message> T load() throws IOException {
        File file = new File(this.path);

        if (!file.exists()) {
            return null;
        }

        final byte[] lenBytes = new byte[4];
        try (final FileInputStream fin = new FileInputStream(file);
                final BufferedInputStream input = new BufferedInputStream(fin)) {
            readBytes(lenBytes, input);
            final int len = Bits.getInt(lenBytes, 0);
            if (len <= 0) {
                throw new IOException("Invalid message fullName.");
            }
            final byte[] nameBytes = new byte[len];
            readBytes(nameBytes, input);
            final String name = new String(nameBytes);
            readBytes(lenBytes, input);
            final int msgLen = Bits.getInt(lenBytes, 0);
            final byte[] msgBytes = new byte[msgLen];
            readBytes(msgBytes, input);
            return ProtobufMsgFactory.newMessageByProtoClassName(name, msgBytes);
        }
    }

    private void readBytes(final byte[] bs, final InputStream input) throws IOException {
        int read;
        if ((read = input.read(bs)) != bs.length) {
            throw new IOException("Read error, expects " + bs.length + " bytes, but read " + read);
        }
    }

    /**
     * Save a protobuf message to file.
     *
     * @param msg  protobuf message
     * @param sync if sync flush data to disk
     * @return true if save success
     */
    public boolean save(final Message msg, final boolean sync) throws IOException {
        // Write message into temp file
        final File file = new File(this.path + ".tmp");
        try (final FileOutputStream fOut = new FileOutputStream(file);
                final BufferedOutputStream output = new BufferedOutputStream(fOut)) {
            final byte[] lenBytes = new byte[4];

            // name len + name
            final String fullName = msg.getDescriptorForType().getFullName();
            final int nameLen = fullName.length();
            Bits.putInt(lenBytes, 0, nameLen);
            output.write(lenBytes);
            output.write(fullName.getBytes());
            // msg len + msg
            final int msgLen = msg.getSerializedSize();
            Bits.putInt(lenBytes, 0, msgLen);
            output.write(lenBytes);
            msg.writeTo(output);
            output.flush();
        }
        if (sync) {
            Utils.fsync(file);
        }

        return Utils.atomicMoveFile(file, new File(this.path), sync);
    }
}
