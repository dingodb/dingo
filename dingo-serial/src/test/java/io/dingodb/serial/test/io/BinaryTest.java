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

package io.dingodb.serial.test.io;

import io.dingodb.serial.io.BinaryDecoder;
import io.dingodb.serial.io.BinaryEncoder;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BinaryTest {

    @Test
    public void testCodeRecord() throws IOException {
        BinaryEncoder be = new BinaryEncoder(new byte[80]);

        be.writeBoolean(false); //length 2
        be.writeBoolean(null);
        be.writeShort((short) 1); //length 3
        be.writeShort(null);
        be.writeInt(1); //length 5
        be.writeInt(null);
        be.writeLong(1L); //length 9
        be.writeLong(null);
        be.writeFloat(1f); //length 5
        be.writeFloat(null);
        be.writeDouble(1d); //length 9
        be.writeDouble(null);
        be.writeString("123", 0);
        be.writeString("测试长度超过80的情况", 0);
        be.writeString("", 0);
        be.writeString(null, 0);

        byte[] result = be.getByteArray();

        BinaryDecoder bd = new BinaryDecoder(result);
        assertEquals(bd.readBoolean(), false);
        assertEquals(bd.readBoolean(), null);
        assertEquals(bd.readShort(), (short) 1);
        assertEquals(bd.readShort(), null);
        assertEquals(bd.readInt(), 1);
        assertEquals(bd.readInt(), null);
        assertEquals(bd.readLong(), 1L);
        assertEquals(bd.readLong(), null);
        assertEquals(bd.readFloat(), 1f);
        assertEquals(bd.readFloat(), null);
        assertEquals(bd.readDouble(), 1d);
        assertEquals(bd.readDouble(), null);
        assertEquals(bd.readString(), "123");
        assertEquals(bd.readString(), "测试长度超过80的情况");
        assertEquals(bd.readString(), "");
        assertEquals(bd.readString(), null);
        assertEquals(bd.remainder(), 0);
    }
}
