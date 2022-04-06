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

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import io.dingodb.common.table.TupleMapping;
import io.dingodb.common.table.TupleSchema;
import io.dingodb.store.api.KeyValue;
import io.dingodb.store.api.PartitionOper;
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

@Slf4j
public final class PartInKvStore implements Part {
    private final PartitionOper block;
    @Getter
    private final KeyValueCodec codec;

    public PartInKvStore(PartitionOper block, TupleSchema schema, TupleMapping keyMapping) {
        this.block = block;
        this.codec = new KeyValueCodec(schema, keyMapping);
    }

    @Override
    @Nonnull
    public Iterator<Object[]> getIterator() {
        return Iterators.transform(block.getIterator(), new Function<KeyValue, Object[]>() {
            @Nullable
            @Override
            public Object[] apply(KeyValue keyValue) {
                try {
                    return codec.decode(keyValue);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }
        });
    }

    @Override
    public boolean insert(@Nonnull Object[] tuple) {
        try {
            KeyValue keyValue = codec.encode(tuple);
            if (!block.contains(keyValue.getKey())) {
                block.put(keyValue);
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public void upsert(@Nonnull Object[] tuple) {
        try {
            KeyValue keyValue = codec.encode(tuple);
            block.put(keyValue);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean remove(@Nonnull Object[] tuple) {
        try {
            KeyValue keyValue = codec.encode(tuple);
            if (block.contains(keyValue.getKey())) {
                block.delete(keyValue.getKey());
                return true;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    @Nullable
    public Object[] getByKey(@Nonnull Object[] keyTuple) {
        try {
            byte[] key = codec.encodeKey(keyTuple);
            byte[] value = block.get(key);
            if (value != null) {
                return codec.mapKeyAndDecodeValue(keyTuple, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    @Nonnull
    public List<Object[]> getByMultiKey(@Nonnull final List<Object[]> keyTuples) {
        List<byte[]> keyList = keyTuples.stream()
            .map(k -> {
                try {
                    return codec.encodeKey(k);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        List<Object[]> tuples = new ArrayList<>(keyList.size());
        try {
            List<byte[]> valueList = block.multiGet(keyList);
            if (keyList.size() != valueList.size()) {
                log.error("Get KeyValues from Store => keyCnt:{} mismatch valueCnt:{}",
                    keyList.size(),
                    valueList.size()
                );
            }
            ListIterator<Object[]> keyIt = keyTuples.listIterator();
            for (byte[] value : valueList) {
                if (value == null) {
                    keyIt.next();
                    continue;
                }
                tuples.add(codec.mapKeyAndDecodeValue(keyIt.next(), value));
            }
        } catch (IOException e) {
            log.error("Get KeyValues from Store => Catch Exception:{} when read data", e.getMessage(), e);
            e.printStackTrace();
        }
        return tuples;
    }
}
