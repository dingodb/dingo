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

package io.dingodb.codec;

import io.dingodb.common.type.TupleMapping;

import java.io.IOException;

public interface Codec {
    byte[] encode(Object[] tuple) throws IOException, ClassCastException;

    byte[] encode(Object[] tuple, TupleMapping mapping) throws IOException, ClassCastException;

    byte[] encode(byte[] origin, Object[] tuple, int[] schemaIndex)
        throws IOException, ClassCastException;

    default byte[] encodeKey(Object[] tuple) throws IOException, ClassCastException {
        return encode(tuple);
    }

    default byte[] encodeKey(Object[] tuple, TupleMapping mapping) throws IOException, ClassCastException {
        return encode(tuple, mapping);
    }

    default byte[] encodeKey(byte[] origin, Object[] tuple, int[] schemaIndex)
        throws IOException, ClassCastException {
        return encode(origin, tuple, schemaIndex);
    }

    default byte[] encodeKeyForRangeScan(Object[] tuple, int columnCount) throws IOException, ClassCastException {
        return encode(tuple);
    }

    Object[] decode(byte[] bytes) throws IOException;

    Object[] decode(Object[] result, byte[] bytes, TupleMapping mapping) throws IOException;

    Object[] decode(byte[] bytes, int[] schemaIndex) throws IOException;

    default Object[] decodeKey(byte[] bytes) throws IOException {
        return decode(bytes);
    }

    default Object[] decodeKey(Object[] result, byte[] bytes, TupleMapping mapping) throws IOException {
        return decode(result, bytes, mapping);
    }

    default Object[] decodeKey(byte[] bytes, int[] schemaIndex) throws IOException {
        return decode(bytes, schemaIndex);
    }
}
