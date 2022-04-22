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

package io.dingodb.raft.storage.impl;

import io.dingodb.raft.conf.Configuration;
import io.dingodb.raft.conf.ConfigurationEntry;
import io.dingodb.raft.conf.ConfigurationManager;
import io.dingodb.raft.entity.EnumOutter.EntryType;
import io.dingodb.raft.entity.LogEntry;
import io.dingodb.raft.entity.LogId;
import io.dingodb.raft.option.LogStorageOptions;
import io.dingodb.raft.storage.LogStorage;
import io.dingodb.raft.util.Bits;
import io.dingodb.raft.util.BytesUtil;
import io.dingodb.raft.util.Describer;
import io.dingodb.raft.util.OnlyForTest;
import io.dingodb.raft.util.Requires;
import io.dingodb.raft.util.Utils;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class RocksDBLogStorage implements LogStorage, Describer {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBLogStorage.class);

    static {
        RocksDB.loadLibrary();
    }

    /**
     * Write batch template.
     */
    private interface WriteBatchTemplate {
        void execute(WriteBatch batch) throws RocksDBException, IOException, InterruptedException;
    }

    /**
     * A write context.
     *
     */
    public interface WriteContext {
        /**
         * Start a sub job.
         */
        default void startJob() {
        }

        /**
         * Finish a sub job.
         */
        default void finishJob() {
        }

        /**
         * Adds a callback that will be invoked after all sub jobs finish.
         */
        default void addFinishHook(final Runnable runnable) {
        }

        /**
         * Set an exception to context.
         * @param exception exception
         */
        default void setError(final Exception exception) {
        }

        /**
         * Wait for all sub jobs finish.
         */
        default void joinAll() throws InterruptedException, IOException {
        }
    }

    /**
     * An empty write context.
     * @author boyan(boyan@antfin.com)
     *
     */
    protected static class EmptyWriteContext implements WriteContext {
        static EmptyWriteContext INSTANCE = new EmptyWriteContext();
    }

    private RocksDBLogStore dbStore;

    private final byte[] regionId;
    /**
     * First log index and last log index key in configuration column family.
     */
    private final byte[] firstLogIndexKey;
    private final byte[] firstLogIndexTail = Utils.getBytes("/meta/firstLogIndex");

    private volatile long firstLogIndex = 1;
    private static HashMap<String, Long> manualCompactMap = new HashMap<>();

    private volatile boolean hasLoadFirstLogIndex;

    public RocksDBLogStorage(byte[] regionId, RocksDBLogStore dbStore) {
        super();
        this.regionId = regionId;
        byte[] firstLogIndexKey = new byte[regionId.length + firstLogIndexTail.length];
        System.arraycopy(regionId, 0, firstLogIndexKey, 0, regionId.length);
        System.arraycopy(firstLogIndexTail, 0, firstLogIndexKey, regionId.length, firstLogIndexTail.length);
        this.firstLogIndexKey = firstLogIndexKey;
        this.dbStore = dbStore;
    }

    @Override
    public boolean init(final LogStorageOptions opts) {
        ConfigurationManager confManager = opts.getConfigurationManager();
        Requires.requireNonNull(confManager, "Null conf manager");
        this.hasLoadFirstLogIndex = false;
        this.firstLogIndex = 1;
        load(confManager);
        return onInitLoaded();
    }

    private void load(final ConfigurationManager confManager) {
        checkState();
        this.dbStore.getWriteLock().lock();
        try (final RocksIterator it = this.dbStore.getDb()
            .newIterator(this.dbStore.getConfHandle(), this.dbStore.getTotalOrderReadOptions())) {
            it.seek(regionId);
            while (it.isValid()) {
                final byte[] ks = it.key();
                final byte[] bs = it.value();
                if (ks.length < regionId.length) {
                    break;
                }
                byte[] pre = new byte[regionId.length];
                System.arraycopy(ks, 0, pre, 0, regionId.length);
                if (!Arrays.equals(pre, regionId)) {
                    break;
                }
                // LogEntry index
                if (ks.length == regionId.length + 8) {
                    final LogEntry entry = this.dbStore.getLogEntryDecoder().decode(bs);
                    if (entry != null) {
                        if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION) {
                            final ConfigurationEntry confEntry = new ConfigurationEntry();
                            confEntry.setId(new LogId(entry.getId().getIndex(), entry.getId().getTerm()));
                            confEntry.setConf(new Configuration(entry.getPeers(), entry.getLearners()));
                            if (entry.getOldPeers() != null) {
                                confEntry.setOldConf(new Configuration(entry.getOldPeers(),
                                    entry.getOldLearners()));
                            }
                            if (confManager != null) {
                                confManager.add(confEntry);
                            }
                        }
                    } else {
                        LOG.warn("Fail to decode conf entry at index {}, the log data is: {}.", Bits.getLong(ks, 0),
                            BytesUtil.toHex(bs));
                    }
                } else {
                    if (Arrays.equals(firstLogIndexKey, ks)) {
                        setFirstLogIndex(Bits.getLong(bs, 0));
                        truncatePrefixInBackground(0L, this.firstLogIndex);
                    } else {
                        LOG.warn("Unknown entry in configuration storage key={}, value={}.",
                            BytesUtil.toHex(ks), BytesUtil.toHex(bs));
                    }
                }
                it.next();
            }
        }
        this.dbStore.getWriteLock().unlock();
    }

    private void setFirstLogIndex(final long index) {
        this.firstLogIndex = index;
        this.hasLoadFirstLogIndex = true;
    }

    /**
     * Save the first log index into conf column family.
     */
    private boolean saveFirstLogIndex(final long firstLogIndex) {
        this.dbStore.getReadLock().lock();
        try {
            final byte[] vs = new byte[8];
            Bits.putLong(vs, 0, firstLogIndex);
            checkState();
            this.dbStore.getDb().put(this.dbStore.getConfHandle(),
                this.dbStore.getWriteOptions(), firstLogIndexKey, vs);
            return true;
        } catch (final RocksDBException e) {
            LOG.error("Fail to save first log index {}, {}.", firstLogIndex, e);
            return false;
        } finally {
            this.dbStore.getReadLock().unlock();
        }
    }

    private void checkState() {
        Requires.requireNonNull(this.dbStore.getDb(),
            "DB not initialized or destroyed");
    }

    /**
     * Execute write batch template.
     *
     * @param template write batch template
     */
    private boolean executeBatch(final WriteBatchTemplate template) {
        this.dbStore.getReadLock().lock();
        if (this.dbStore.getDb() == null) {
            LOG.warn("DB not initialized or destroyed.");
            this.dbStore.getReadLock().unlock();
            return false;
        }
        try (final WriteBatch batch = new WriteBatch()) {
            template.execute(batch);
            this.dbStore.getDb().write(this.dbStore.getWriteOptions(), batch);
        } catch (final RocksDBException e) {
            LOG.error("Execute batch failed with rocksdb exception.", e);
            return false;
        } catch (final IOException e) {
            LOG.error("Execute batch failed with io exception.", e);
            return false;
        } catch (final InterruptedException e) {
            LOG.error("Execute batch failed with interrupt.", e);
            Thread.currentThread().interrupt();
            return false;
        } finally {
            this.dbStore.getReadLock().unlock();
        }
        return true;
    }

    @Override
    public void shutdown() {
        this.dbStore.getWriteLock().lock();
        try {
            onShutdown();
        } catch (Exception e) {
            LOG.error("Fail to delete LogData on LogStorage RegionId = {}",
                new String(this.regionId, StandardCharsets.UTF_8));
        } finally {
            this.dbStore.getWriteLock().unlock();
        }
    }

    @Override
    public long getFirstLogIndex() {
        this.dbStore.getReadLock().lock();
        try {
            return getFirstLogIndexNoLock();
        } finally {
            this.dbStore.getReadLock().unlock();
        }
    }

    private long getFirstLogIndexNoLock() {
        if (this.hasLoadFirstLogIndex) {
            return this.firstLogIndex;
        }
        checkState();
        try (final RocksIterator it = this.dbStore.getDb().newIterator(this.dbStore.getDefaultHandle(),
            this.dbStore.getTotalOrderReadOptions())) {
            it.seek(regionId);
            if (it.isValid()) {
                byte[] key = it.key();
                if (key.length < regionId.length) {
                    return 1L;
                }
                byte[] pre = new byte[regionId.length];
                System.arraycopy(key, 0, pre, 0, regionId.length);
                if (Arrays.equals(pre, regionId)) {
                    final long ret = Bits.getLong(key, regionId.length);
                    saveFirstLogIndex(ret);
                    setFirstLogIndex(ret);
                    return ret;
                }
            }
            return 1L;
        }
    }

    @Override
    public long getLastLogIndex() {
        this.dbStore.getReadLock().lock();
        try {
            return getLastLogIndexNoLock();
        } finally {
            this.dbStore.getReadLock().unlock();
        }
    }

    private long getLastLogIndexNoLock() {
        checkState();
        try (final RocksIterator it = this.dbStore.getDb().newIterator(this.dbStore.getDefaultHandle(),
            this.dbStore.getTotalOrderReadOptions())) {
            byte[] maxIndex = new byte[regionId.length + 8];
            System.arraycopy(regionId, 0, maxIndex, 0, regionId.length);
            Bits.putLong(maxIndex, regionId.length, Long.MAX_VALUE);
            it.seekForPrev(maxIndex);
            if (it.isValid()) {
                byte[] key = it.key();
                if (key.length < regionId.length) {
                    return 0L;
                }
                byte[] pre = new byte[regionId.length];
                System.arraycopy(key, 0 , pre, 0, regionId.length);
                if (Arrays.equals(pre, regionId)) {
                    return Bits.getLong(key, regionId.length);
                }
            }
            return 0L;
        }
    }

    @Override
    public LogEntry getEntry(final long index) {
        this.dbStore.getReadLock().lock();
        try {
            if (this.hasLoadFirstLogIndex && index < this.firstLogIndex) {
                return null;
            }
            return getEntryFromDB(index);
        } catch (final RocksDBException | IOException e) {
            LOG.error("Fail to get log entry at index {}, {}.", index, e);
        } finally {
            this.dbStore.getReadLock().unlock();
        }
        return null;
    }

    @OnlyForTest
    LogEntry getEntryFromDB(final long index) throws IOException, RocksDBException {
        final byte[] keyBytes = getKeyBytes(index);
        final byte[] bs = onDataGet(index, getValueFromRocksDB(keyBytes));
        if (bs != null) {
            final LogEntry entry = this.dbStore.getLogEntryDecoder().decode(bs);
            if (entry != null) {
                return entry;
            } else {
                LOG.error("Bad log entry format for index={}, the log data is: {}.", index, BytesUtil.toHex(bs));
                // invalid data remove? TODO
                return null;
            }
        }
        return null;
    }

    protected byte[] getValueFromRocksDB(final byte[] keyBytes) throws RocksDBException {
        checkState();
        return this.dbStore.getDb().get(this.dbStore.getDefaultHandle(), keyBytes);
    }

    protected byte[] getKeyBytes(final long index) {
        byte[] ks = new byte[8 + regionId.length];
        System.arraycopy(regionId, 0, ks, 0, regionId.length);
        Bits.putLong(ks, regionId.length, index);
        return ks;
    }

    @Override
    public long getTerm(final long index) {
        final LogEntry entry = getEntry(index);
        if (entry != null) {
            return entry.getId().getTerm();
        }
        return 0;
    }

    private void addConfBatch(final LogEntry entry, final WriteBatch batch) throws RocksDBException {
        final byte[] ks = getKeyBytes(entry.getId().getIndex());
        final byte[] content = this.dbStore.getLogEntryEncoder().encode(entry);
        batch.put(this.dbStore.getDefaultHandle(), ks, content);
        batch.put(this.dbStore.getConfHandle(), ks, content);
    }

    private void addDataBatch(final LogEntry entry, final WriteBatch batch,
                              final WriteContext ctx) throws RocksDBException, IOException, InterruptedException {
        final long logIndex = entry.getId().getIndex();
        final byte[] content = this.dbStore.getLogEntryEncoder().encode(entry);
        batch.put(this.dbStore.getDefaultHandle(), getKeyBytes(logIndex), onDataAppend(logIndex, content, ctx));
    }

    @Override
    public boolean appendEntry(final LogEntry entry) {
        if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION || entry.getType() == EntryType.ENTRY_TYPE_MSG) {
            return executeBatch(batch -> addConfBatch(entry, batch));
        } else {
            this.dbStore.getReadLock().lock();
            try {
                if (this.dbStore.getDb() == null) {
                    LOG.warn("DB not initialized or destroyed.");
                    return false;
                }
                final WriteContext writeCtx = newWriteContext();
                final long logIndex = entry.getId().getIndex();
                final byte[] valueBytes = this.dbStore.getLogEntryEncoder().encode(entry);
                final byte[] newValueBytes = onDataAppend(logIndex, valueBytes, writeCtx);
                writeCtx.startJob();
                this.dbStore.getDb().put(this.dbStore.getDefaultHandle(),
                    this.dbStore.getWriteOptions(), getKeyBytes(logIndex), newValueBytes);
                writeCtx.joinAll();
                if (newValueBytes != valueBytes) {
                    doSync();
                }
                return true;
            } catch (final RocksDBException | IOException e) {
                LOG.error("Fail to append entry.", e);
                return false;
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } finally {
                this.dbStore.getReadLock().unlock();
            }
        }
    }

    private void doSync() throws IOException, InterruptedException {
        onSync();
    }

    @Override
    public int appendEntries(final List<LogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return 0;
        }
        final int entriesCount = entries.size();
        final boolean ret = executeBatch(batch -> {
            final WriteContext writeCtx = newWriteContext();
            for (int i = 0; i < entriesCount; i++) {
                final LogEntry entry = entries.get(i);
                if (entry.getType() == EntryType.ENTRY_TYPE_CONFIGURATION
                    || entry.getType() == EntryType.ENTRY_TYPE_MSG ) {
                    addConfBatch(entry, batch);
                } else {
                    writeCtx.startJob();
                    addDataBatch(entry, batch, writeCtx);
                }
            }
            writeCtx.joinAll();
            doSync();
        });

        if (ret) {
            return entriesCount;
        } else {
            return 0;
        }
    }

    @Override
    public boolean truncatePrefix(final long firstIndexKept) {
        this.dbStore.getReadLock().lock();
        try {
            final long startIndex = getFirstLogIndex();
            final boolean ret = saveFirstLogIndex(firstIndexKept);
            if (ret) {
                setFirstLogIndex(firstIndexKept);
            }
            truncatePrefixInBackground(startIndex, firstIndexKept);
            return ret;
        } finally {
            this.dbStore.getReadLock().unlock();
        }

    }

    private void truncatePrefixInBackground(final long startIndex, final long firstIndexKept) {
        // delete logs in background.
        Utils.runInThread(() -> {
            long startMs = Utils.monotonicMs();
            this.dbStore.getReadLock().lock();
            try {
                if (this.dbStore.getDb() == null) {
                    LOG.warn(
                        "DB is null while truncating prefixed logs, the range is: [{}, {})",
                        startIndex, firstIndexKept);
                    return;
                }
                onTruncatePrefix(startIndex, firstIndexKept);
                // Note https://github.com/facebook/rocksdb/wiki/Delete-A-Range-Of-Keys
                final byte[] startKey = getKeyBytes(startIndex);
                final byte[] endKey = getKeyBytes(firstIndexKept);

                // deleteRange to delete all keys in range.
                this.dbStore.getDb().deleteRange(this.dbStore.getDefaultHandle(),
                    startKey, endKey);
                this.dbStore.getDb().deleteRange(this.dbStore.getConfHandle(),
                    startKey, endKey);
                // deleteFilesInRanges to speedup reclaiming disk space on write-heavy load.
                this.dbStore.getDb().deleteFilesInRanges(this.dbStore.getDefaultHandle(),
                    Arrays.asList(startKey, endKey), false);
                this.dbStore.getDb().deleteFilesInRanges(this.dbStore.getConfHandle(),
                    Arrays.asList(startKey, endKey), false);
            } catch (final RocksDBException | IOException e) {
                LOG.error("Fail to truncatePrefix, firstIndexKept={}.", firstIndexKept, e);
            } finally {
                this.dbStore.getReadLock().unlock();
                LOG.info("Truncated prefix logs from log index {} to {}, cost {} ms.",
                    startIndex, firstIndexKept, Utils.monotonicMs() - startMs);
            }
        });
    }

    @Override
    public boolean truncateSuffix(final long lastIndexKept) {
        this.dbStore.getReadLock().lock();
        Long startMS = System.nanoTime();
        try {
            try {
                onTruncateSuffix(lastIndexKept);
            } finally {
                Long lastIndex = getLastLogIndex();
                this.dbStore.getDb().deleteRange(this.dbStore.getDefaultHandle(),
                    this.dbStore.getWriteOptions(), getKeyBytes(lastIndexKept + 1),
                    getKeyBytes(getLastLogIndex() + 1));
                this.dbStore.getDb().deleteRange(this.dbStore.getConfHandle(),
                    this.dbStore.getWriteOptions(), getKeyBytes(lastIndexKept + 1),
                    getKeyBytes(getLastLogIndex() + 1));

                //Long times = doCompactByTimes(this.dbStore.getPath());
                Long endMS = System.nanoTime();
                LOG.debug("truncate Suffix: dbPath:{}, last startIndex:{}, "
                        + "endIndex:{} diff:{}, cost:{}.",
                    this.dbStore.getPath(),
                    lastIndexKept,
                    lastIndex,
                    (lastIndex - lastIndexKept),
                    (endMS - startMS) / 1000 / 1000
                );
            }
            return true;
        } catch (final RocksDBException | IOException e) {
            LOG.error("Fail to truncateSuffix {}, {}.", lastIndexKept, e);
        } finally {
            this.dbStore.getReadLock().unlock();
        }
        return false;
    }

    /**
     * internal trigger rocksdb compaction.
     * @param path input db path
     * @return compactTimes
     * @throws RocksDBException rockdbException
     */
    private Long doCompactByTimes(final String path) throws RocksDBException {
        Long times = manualCompactMap.get(this.dbStore.getPath());
        if (times != null) {
            manualCompactMap.put(this.dbStore.getPath(), ++times);
        } else {
            times = 1L;
            manualCompactMap.put(this.dbStore.getPath(), times);
        }

        // todo Huzx
        /*
        this.db.compactRange();
         */
        return times;
    }

    @Override
    public boolean reset(final long nextLogIndex) {
        if (nextLogIndex <= 0) {
            throw new IllegalArgumentException("Invalid next log index.");
        }
        this.dbStore.getWriteLock().lock();
        try {
            LogEntry entry = getEntry(nextLogIndex);
            onReset(nextLogIndex);
            if (entry == null) {
                entry = new LogEntry();
                entry.setType(EntryType.ENTRY_TYPE_NO_OP);
                entry.setId(new LogId(nextLogIndex, 0));
                LOG.warn("Entry not found for nextLogIndex {} when reset.", nextLogIndex);
            }
            return appendEntry(entry);
        } finally {
            this.dbStore.getWriteLock().unlock();
        }
    }

    // Hooks for {@link RocksDBSegmentLogStorage}

    /**
     * Called after opening RocksDB and loading configuration into conf manager.
     */
    protected boolean onInitLoaded() {
        return true;
    }

    /**
     * Called after closing db.
     */
    protected void onShutdown() throws Exception {
        this.dbStore.getDb().deleteRange(getKeyBytes(getFirstLogIndexNoLock()), getKeyBytes(getLastLogIndexNoLock()));
    }

    /**
     * Called after resetting db.
     *
     * @param nextLogIndex next log index
     */
    protected void onReset(final long nextLogIndex) {
    }

    /**
     * Called after truncating prefix logs in rocksdb.
     *
     * @param startIndex     the start index
     * @param firstIndexKept the first index to kept
     */
    protected void onTruncatePrefix(final long startIndex, final long firstIndexKept) throws RocksDBException,
        IOException {
    }

    /**
     * Called when sync data into file system.
     */
    protected void onSync() throws IOException, InterruptedException {
    }

    protected boolean isSync() {
        return this.dbStore.isSync();
    }

    /**
     * Called after truncating suffix logs in rocksdb.
     *
     * @param lastIndexKept the last index to kept
     */
    protected void onTruncateSuffix(final long lastIndexKept) throws RocksDBException, IOException {
    }

    protected WriteContext newWriteContext() {
        return EmptyWriteContext.INSTANCE;
    }

    /**
     * Called before appending data entry.
     *
     * @param logIndex the log index
     * @param value    the data value in log entry.
     * @return the new value
     */
    protected byte[] onDataAppend(final long logIndex, final byte[] value,
                                  final WriteContext ctx) throws IOException, InterruptedException {
        ctx.finishJob();
        return value;
    }

    /**
     * Called after getting data from rocksdb.
     *
     * @param logIndex the log index
     * @param value    the value in rocksdb
     * @return the new value
     */
    protected byte[] onDataGet(final long logIndex, final byte[] value) throws IOException {
        return value;
    }

    @Override
    public void describe(final Printer out) {
        this.dbStore.getReadLock().lock();
        try {
            if (this.dbStore.getDb() != null) {
                out.println(this.dbStore.getDb().getProperty("rocksdb.stats"));
            }
            out.println("");
            if (this.dbStore.getStatistics() != null) {
                out.println(this.dbStore.getStatistics().getString());
            }
        } catch (final RocksDBException e) {
            out.println(e);
        } finally {
            this.dbStore.getReadLock().unlock();
        }
    }
}
