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

package io.dingodb.store.row;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Bytes;
import io.dingodb.store.api.KeyValue;
import io.dingodb.store.api.PartitionOper;
import io.dingodb.store.row.client.DefaultDingoRowStore;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Slf4j
public final class RowPartitionOper implements PartitionOper {
    private final DefaultDingoRowStore kvStore;
    private final byte[] keyPrefix;

    public RowPartitionOper(DefaultDingoRowStore kvStore, byte[] keyPrefix) {
        this.kvStore = kvStore;
        this.keyPrefix = keyPrefix;
        create();
    }

    @Override
    public void create() {
    }

    @Override
    @Nonnull
    public Iterator<KeyValue> getIterator() {
        return new RowBlockIterator(kvStore, keyPrefix);
    }

    private byte[] prefixedKey(byte[] key) {
        return Bytes.concat(keyPrefix, key);
    }

    public boolean contains(byte[] key) {
        try {
            return kvStore.containsKey(prefixedKey(key)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public void put(@Nonnull KeyValue keyValue) {
        try {
            kvStore.put(prefixedKey(keyValue.getKey()), keyValue.getValue()).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void delete(byte[] key) {
        try {
            kvStore.delete(prefixedKey(key)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Nullable
    @Override
    public byte[] get(byte[] key) {
        try {
            return kvStore.get(prefixedKey(key)).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    @Nonnull
    public List<byte[]> multiGet(@Nonnull final List<byte[]> keys) {
        try {
            return new ArrayList<>(kvStore.multiGet(
                keys.stream().map(this::prefixedKey).collect(Collectors.toList())
            ).get().values());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ImmutableList.of();
    }
}
