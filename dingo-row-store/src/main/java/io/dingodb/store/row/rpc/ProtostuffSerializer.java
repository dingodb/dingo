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

package io.dingodb.store.row.rpc;

import com.alipay.remoting.exception.CodecException;
import com.alipay.remoting.serialization.Serializer;
import io.dingodb.raft.util.internal.ThrowUtil;
import io.dingodb.store.row.serialization.Serializers;
import io.dingodb.store.row.util.Maps;

import java.util.concurrent.ConcurrentMap;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class ProtostuffSerializer implements Serializer {
    public static final ProtostuffSerializer INSTANCE = new ProtostuffSerializer();

    private static final ConcurrentMap<String, Class<?>> classCache = Maps.newConcurrentMap();

    private final io.dingodb.store.row.serialization.Serializer delegate
        = Serializers.getSerializer(Serializers.PROTO_STUFF);

    @Override
    public byte[] serialize(final Object obj) throws CodecException {
        return this.delegate.writeObject(obj);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T deserialize(final byte[] data, final String classOfT) throws CodecException {
        Class<?> clazz = classCache.get(classOfT);
        if (clazz == null) {
            try {
                final Class<?> newClazz = Class.forName(classOfT);
                clazz = classCache.putIfAbsent(classOfT, newClazz);
                if (clazz == null) {
                    clazz = newClazz;
                }
            } catch (final Exception e) {
                ThrowUtil.throwException(e);
            }
        }
        return (T) this.delegate.readObject(data, clazz);
    }
}
