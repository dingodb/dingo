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

package io.dingodb.exec.table;

import com.google.common.collect.Iterators;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.table.KeyValueCodec;
import io.dingodb.common.table.Part;
import io.dingodb.common.table.TupleMapping;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.store.api.StoreInstance;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static io.dingodb.common.util.NoBreakFunctionWrapper.wrap;

@Slf4j
public final class PartInKvStore implements Part {
    private final StoreInstance store;
    @Getter
    private final KeyValueCodec codec;

    public PartInKvStore(StoreInstance store, TupleSchema schema, TupleMapping keyMapping) {
        this.store = store;
        this.codec = new KeyValueCodec(schema, keyMapping);
    }

    @Override
    @Nonnull
    public Iterator<Object[]> getIterator() {
        return Iterators.transform(
            store.keyValueScan(),
            wrap(codec::decode, e -> log.error("Iterator: decode error.", e))::apply
        );
    }

    @Override
    public boolean insert(@Nonnull Object[] tuple) {
        try {
            KeyValue row = codec.encode(tuple);
            if (!store.exist(row.getPrimaryKey())) {
                store.upsertKeyValue(row);
                return true;
            }
        } catch (IOException e) {
            log.error("Insert: encode error.", e);
        }
        return false;
    }

    @Override
    public void upsert(@Nonnull Object[] tuple) {
        try {
            KeyValue row = codec.encode(tuple);
            store.upsertKeyValue(row);
        } catch (IOException e) {
            log.error("Upsert: encode error.", e);
        }
    }

    @Override
    public boolean remove(@Nonnull Object[] tuple) {
        try {
            KeyValue row = codec.encode(tuple);
            return store.delete(row.getPrimaryKey());
        } catch (IOException e) {
            log.error("Remove: encode error.", e);
        }
        return false;
    }

    @Override
    @Nullable
    public Object[] getByKey(@Nonnull Object[] keyTuple) {
        try {
            byte[] key = codec.encodeKey(keyTuple);
            byte[] value = store.getValueByPrimaryKey(key);
            if (value != null) {
                return codec.mapKeyAndDecodeValue(keyTuple, value);
            }
        } catch (IOException e) {
            log.error("GetByKey: codec error.", e);
        }
        return null;
    }

    @Override
    @Nonnull
    public List<Object[]> getByMultiKey(@Nonnull final List<Object[]> keyTuples) {
        List<byte[]> keyList = keyTuples.stream()
            .map(wrap(codec::encodeKey, e -> log.error("GetByMultiKey: encode key error.", e)))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        List<Object[]> tuples = new ArrayList<>(keyList.size());
        try {
            List<KeyValue> valueList = store.getKeyValueByPrimaryKeys(keyList);
            if (keyList.size() != valueList.size()) {
                log.error("Get KeyValues from Store => keyCnt:{} mismatch valueCnt:{}",
                    keyList.size(),
                    valueList.size()
                );
            }
            ListIterator<Object[]> keyIt = keyTuples.listIterator();
            for (KeyValue row : valueList) {
                if (row == null) {
                    keyIt.next();
                    continue;
                }
                tuples.add(codec.mapKeyAndDecodeValue(keyIt.next(), row.getValue()));
            }
        } catch (IOException e) {
            log.error("Get KeyValues from Store => Catch Exception:{} when read data", e.getMessage(), e);
        }
        return tuples;
    }
}
