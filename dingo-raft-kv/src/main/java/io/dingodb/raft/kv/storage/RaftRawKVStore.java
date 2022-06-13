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

package io.dingodb.raft.kv.storage;

import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.util.Files;
import io.dingodb.raft.Lifecycle;
import io.dingodb.raft.Node;
import io.dingodb.raft.NodeManager;
import io.dingodb.raft.conf.Configuration;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.option.NodeOptions;
import io.dingodb.raft.storage.LogStorage;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.zip.Checksum;

import static io.dingodb.raft.RaftServiceFactory.createRaftNode;

@Slf4j
@Getter
public class RaftRawKVStore implements Lifecycle<Void> {

    private final CommonId raftId;

    private Path path;
    private Path metaPath;
    private Path snapshotPath;

    private final Node node;
    private final RawKVStore kvStore;
    private final NodeOptions nodeOptions;
    private final ReadIndexRunner readIndexRunner;

    public RaftRawKVStore(CommonId raftId, RawKVStore kvStore, NodeOptions nodeOptions, Location location) {
        this.raftId = raftId;
        this.node = createRaftNode(raftId.toString(), new PeerId(location.getHost(), location.getPort()));
        this.kvStore = kvStore;
        this.nodeOptions = nodeOptions;
        this.readIndexRunner = new ReadIndexRunner(node, this::executeLocal);
    }

    public RaftRawKVStore(
        CommonId raftId,
        RawKVStore kvStore,
        NodeOptions nodeOptions,
        Path path,
        LogStorage logStorage,
        Location location,
        List<Location> locations
    ) {
        this.raftId = raftId;
        this.path = path;
        this.metaPath = Paths.get(path.toString(), "meta");
        this.snapshotPath = Paths.get(path.toString(), "snapshot");
        Files.createDirectories(metaPath);
        Files.createDirectories(snapshotPath);
        Files.createDirectories(metaPath);
        Files.createDirectories(snapshotPath);
        if (nodeOptions == null) {
            nodeOptions = new NodeOptions();
        } else {
            nodeOptions = nodeOptions.copy();
        }
        nodeOptions.setLogStorage(logStorage);
        nodeOptions.setInitialConf(new Configuration(locations.stream()
            .map(l -> new PeerId(l.getHost(), l.getRaftPort()))
            .collect(Collectors.toList())));
        nodeOptions.setRaftMetaUri(metaPath.toString());
        nodeOptions.setSnapshotUri(snapshotPath.toString());
        this.node = createRaftNode(raftId.toString(), new PeerId(location.getHost(), location.getPort()));
        this.kvStore = kvStore;
        this.nodeOptions = nodeOptions;
        this.readIndexRunner = new ReadIndexRunner(node, this::executeLocal);
    }

    @Override
    public boolean init(Void opts) {
        if (nodeOptions.getFsm() == null) {
            nodeOptions.setFsm(new DefaultRaftRawKVStoreStateMachine(UUID.randomUUID().toString(), this));
        }
        NodeManager.getInstance().addAddress(node.getNodeId().getPeerId().getEndpoint());
        return this.node.init(nodeOptions);
    }

    @Override
    public void shutdown() {
        this.node.shutdown(status -> {
            log.info("The {} shutdown, status: {}.", raftId, status);
            Files.deleteIfExists(path);
        });
    }

    protected SeekableIterator<byte[], ByteArrayEntry> localIterator() {
        return this.kvStore.iterator();
    }

    protected byte[] localGet(final byte[] key) {
        return this.kvStore.get(key);
    }

    protected List<ByteArrayEntry> localMultiGet(final List<byte[]> keys) {
        return this.kvStore.get(keys);
    }

    protected Boolean localContainsKey(final byte[] key) {
        return this.kvStore.containsKey(key);
    }

    protected SeekableIterator<byte[], ByteArrayEntry> localScan(final byte[] startKey, final byte[] endKey) {
        return this.kvStore.scan(startKey, endKey);
    }

    protected void localPut(final byte[] key, final byte[] value) {
        this.kvStore.put(key, value);
    }

    protected void localPut(final List<ByteArrayEntry> entries) {
        this.kvStore.put(entries);
    }

    protected Boolean localDelete(final byte[] key) {
        return this.kvStore.delete(key);
    }

    protected Boolean localDelete(final List<byte[]> keys) {
        return this.kvStore.delete(keys);
    }

    protected Boolean localDeleteRange(final byte[] startKey, final byte[] endKey) {
        return this.kvStore.delete(startKey, endKey);
    }

    protected CompletableFuture<Checksum> snapshotSave(RaftRawKVOperation operation) {
        return this.kvStore.snapshotSave(
            operation.ext1(),
            operation.getKey(),
            operation.getExtKey()
        );
    }

    protected CompletableFuture<Boolean> snapshotLoad(RaftRawKVOperation operation) {
        return this.kvStore.snapshotLoad(
            operation.ext1(),
            operation.ext2(),
            operation.getKey(),
            operation.getExtKey()
        );
    }

    public PhaseCommitAck sync() {
        write(RaftRawKVOperation.SYNC_OP).join();
        return new PhaseCommitAck().complete();
    }

    public CompletableFuture<byte[]> get(byte[] key) {
        return read(RaftRawKVOperation.get(key));
    }

    public CompletableFuture<List<ByteArrayEntry>> get(List<byte[]> keys) {
        return read(RaftRawKVOperation.get(keys));
    }

    public CompletableFuture<SeekableIterator<byte[], ByteArrayEntry>> iterator() {
        return read(RaftRawKVOperation.iterator());
    }

    public CompletableFuture<Boolean> containsKey(byte[] key) {
        return read(RaftRawKVOperation.containsKey(key));
    }

    public CompletableFuture<SeekableIterator<byte[], ByteArrayEntry>> scan(byte[] startKey, byte[] endKey) {
        return read(RaftRawKVOperation.scan(startKey, endKey));
    }

    public CompletableFuture<Boolean> put(byte[] key, byte[] value) {
        return write(RaftRawKVOperation.put(key, value));
    }

    public CompletableFuture<Boolean> put(List<ByteArrayEntry> entries) {
        return write(RaftRawKVOperation.put(entries));
    }

    public CompletableFuture<Boolean> delete(byte[] key) {
        return write(RaftRawKVOperation.delete(key));
    }

    public CompletableFuture<Boolean> delete(List<byte[]> keys) {
        return write(RaftRawKVOperation.delete(keys));
    }

    public CompletableFuture<Boolean> delete(byte[] startKey, byte[] endKey) {
        return write(RaftRawKVOperation.delete(startKey, endKey));
    }

    public Object executeLocal(RaftRawKVOperation operation) {
        switch (operation.getOp()) {
            case SYNC:
                return true;
            case PUT:
                localPut(operation.getKey(), operation.getValue());
                return true;
            case PUT_LIST:
                localPut(operation.ext1());
                return true;
            case DELETE:
                return localDelete(operation.getKey());
            case DELETE_LIST:
                return localDelete((List<byte[]>) operation.ext1());
            case DELETE_RANGE:
                return localDeleteRange(operation.getKey(), operation.getExtKey());
            case GET:
                return localGet(operation.getKey());
            case MULTI_GET:
                return localMultiGet(operation.ext1());
            case ITERATOR:
                return localIterator();
            case SCAN:
                return localScan(operation.getKey(), operation.getExtKey());
            case CONTAINS_KEY:
                return localContainsKey(operation.getKey());
            case SNAPSHOT_SAVE:
                return snapshotSave(operation);
            case SNAPSHOT_LOAD:
                return snapshotLoad(operation);
            default:
                throw new IllegalStateException("Unexpected value: " + operation.getOp());
        }
    }

    public <T> CompletableFuture<T> write(RaftRawKVOperation operation) {
        return operation.applyOnNode(node);
    }

    public <T> CompletableFuture<T> read(RaftRawKVOperation operation) {
        return readIndexRunner.readIndex(operation);
    }

}
