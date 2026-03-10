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

package io.dingodb.calcite.meta;

import io.dingodb.common.log.LogUtils;
import io.dingodb.common.memory.AppendingObjectOutputStream;
import io.dingodb.common.memory.ObjectSizeUtils;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class DingoSortedMultiMap<K, V> extends HashMap<K, List<V>> {

    private Map<K, AtomicLong> keySizeMap;

    private Map<K, String> keyFileMap;
    private long spillSize;
    private String basePath;

    public DingoSortedMultiMap() {
        keySizeMap = new HashMap<>();
        this.keyFileMap = new HashMap<>();
        spillSize = 10000000;
        basePath = "/root/gjn/logs/";
    }

    public void putMulti(K key, V value) {
        if (value instanceof Object[]) {
            long size = ObjectSizeUtils.calculateObjectSize(value);
            keySizeMap.compute(key, (k, oldVal) -> {
                if (oldVal == null) {
                    return new AtomicLong(size);
                } else {
                    oldVal.addAndGet(size);
                    return oldVal;
                }
            });
        }
        List<V> list = (List)this.put(key, Collections.singletonList(value));
        if (list != null) {
            if (((List)list).size() == 1) {
                list = new ArrayList((Collection)list);
            }

            ((List)list).add(value);
            this.put(key, list);
            if (value instanceof Object[] && keySizeMap.get(key).get() > spillSize) {
                try {
                    if (this.keyFileMap.containsKey(key)) {
                        appendListToFile(this.keyFileMap.get(key), list);
                    } else {
                        String path = basePath + UUID.randomUUID();
                        appendListToFile(path, list);
                        this.keyFileMap.put(key, path);
                    }
                    keySizeMap.get(key).set(0);
                    list.clear();
                    this.put(key, list);
                } catch (Exception e) {
                    LogUtils.error(log, e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public List<V> put(K key, List<V> value) {
        this.keySizeMap.put(key, new AtomicLong(value.size()));
        return super.put(key, value);
    }

    public Iterator<V[]> arrays(final Comparator<V> comparator) {
        final Iterator<K> iterator = this.keySizeMap.keySet().iterator();
        return new Iterator<V[]>() {
            public boolean hasNext() {
                return iterator.hasNext();
            }

            public V[] next() {
                K key = (K)iterator.next();
                List<V> list;
                if (keyFileMap.containsKey(key)) {
                    // combine memory list and file list
                    list = getOrDefault(key, new ArrayList<>());
                    String path = keyFileMap.get(key);
                    List<V> diskList = readListFromDisk(path);
                    list.addAll(diskList);
                } else {
                    // get memory list
                    list = getOrDefault(key, new ArrayList<>());
                }
                if (list == null) {
                    list = new ArrayList<>();
                }
                V[] vs = (V[]) list.toArray();
                Arrays.sort(vs, comparator);
                return vs;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public void clear() {
        // if spill -> clean rocksdb
        super.clear();
        this.keySizeMap.clear();
        this.keyFileMap.values().forEach(path -> {
            File file = new File(path);
            file.deleteOnExit();
        });
        this.keyFileMap.clear();
    }

    public void appendListToFile(String filePath, List<V> list) throws IOException {
        File file = new File(filePath);
        boolean fileExists = file.exists();

        try (FileOutputStream fos = new FileOutputStream(file, true)) {
            if (!fileExists) {
                try (ObjectOutputStream oos = new ObjectOutputStream(fos)) {
                    oos.writeObject(list);
                    oos.flush();
                }
            } else {
                try (AppendingObjectOutputStream aoos = new AppendingObjectOutputStream(fos)) {
                    aoos.writeObject(list);
                    aoos.flush();
                }
            }
        }
    }

    public static <V> Iterator<V[]> singletonArrayIterator(Comparator<V> comparator, List<V> list) {
        DingoSortedMultiMap<Object, V> multiMap = new DingoSortedMultiMap();
        multiMap.put("x", list);
        return multiMap.arrays(comparator);
    }

    public List<V> readListFromDisk(String path) {
        List<V> allBatches = new ArrayList<>();
        try (FileInputStream fis = new FileInputStream(path);
             ObjectInputStream ois = new ObjectInputStream(fis)) {
            while (true) {
                try {
                    List<V> list = (List<V>) ois.readObject();
                    allBatches.addAll(list);
                } catch (Exception e) {
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return allBatches;
    }

}
