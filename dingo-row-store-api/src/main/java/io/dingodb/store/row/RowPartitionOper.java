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
import io.dingodb.store.api.KeyValue;
import io.dingodb.store.api.PartitionOper;
import io.dingodb.store.row.client.DefaultDingoRowStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Slf4j
public final class RowPartitionOper implements PartitionOper {
    private final String path;

    private DefaultDingoRowStore kvStore;

    public RowPartitionOper(String path, DefaultDingoRowStore kvStore) {
        this.path = path;
        this.kvStore = kvStore;
        create();
    }

    @Nonnull
    private String dataDir() {
        return path + File.separator + "_data";
    }

    @Override
    public void create() {
        try {
            FileUtils.forceMkdir(new File(path));
        } catch (IOException e) {
            throw new RuntimeException("Cannot create dir.", e);
        }
    }

    @Override
    @Nonnull
    public Iterator<KeyValue> getIterator() {
        return new RowBlockIterator(kvStore);
    }

    public boolean contains(byte[] key) {
        try {
            return kvStore.containsKey(key).get();
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
            kvStore.put(keyValue.getKey(), keyValue.getValue()).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void delete(byte[] key) {
        try {
            kvStore.delete(key).get();
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
            return kvStore.get(key).get();
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
            return new ArrayList<>(kvStore.multiGet(keys).get().values());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return ImmutableList.of();
    }
}
