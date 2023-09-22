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

package io.dingodb.partition.base;
import lombok.extern.slf4j.Slf4j;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

@Slf4j
public class ConsistentHashing<T> {
    private final int replicas;
    private final TreeMap<BigInteger, T> ring = new TreeMap<>();

    public ConsistentHashing(int replicas) {
        this.replicas = replicas;
    }

    public void addNode(T node) {
        for (int i = 0; i < replicas; i++) {
            String nodeStr = node.toString() + "-" + i;
            byte[] key = getHash(nodeStr);
            BigInteger hash = new BigInteger(key);
            log.trace("node:" + nodeStr + ",hash:" + hash);
            ring.put(hash, node);
        }
    }

    public void removeNode(T node) {
        for (int i = 0; i < replicas; i++) {
            byte[] key = getHash(node.toString() + "-" + i);
            BigInteger hash = new BigInteger(key);
            ring.remove(hash);
        }
    }

    public T getNode(byte[] key) {
        if (ring.isEmpty()) {
            return null;
        }
        byte[] hash = getHash(key);
        BigInteger bigHash = new BigInteger(hash);
        Map.Entry<BigInteger, T> entry = ring.ceilingEntry(bigHash);
        if (entry == null) {
            entry = ring.firstEntry();
        }
        log.trace("key:" +new String(key) + " hash:" + entry.getKey());
        return entry.getValue();
    }

    private byte[] getHash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(key.getBytes(StandardCharsets.UTF_8));
            return md.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not supported", e);
        }
    }

    private byte[] getHash(byte[] key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(key);
            return md.digest();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not supported", e);
        }
    }

}
