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

package io.dingodb.store.row.serialization;

import io.dingodb.store.row.serialization.impl.protostuff.ProtoStuffSerializer;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public final class Serializers {
    public static final byte PROTO_STUFF = 0x01;

    private static Serializer[] serializers = new Serializer[5];

    static {
        addSerializer(PROTO_STUFF, new ProtoStuffSerializer());
    }

    public static Serializer getSerializer(final int idx) {
        return serializers[idx];
    }

    public static Serializer getDefault() {
        return serializers[PROTO_STUFF];
    }

    @SuppressWarnings("SameParameterValue")
    private static void addSerializer(final int idx, final Serializer serializer) {
        if (serializers.length <= idx) {
            final Serializer[] newSerializers = new Serializer[idx + 5];
            System.arraycopy(serializers, 0, newSerializers, 0, serializers.length);
            serializers = newSerializers;
        }
        serializers[idx] = serializer;
    }

    private Serializers() {
    }
}
