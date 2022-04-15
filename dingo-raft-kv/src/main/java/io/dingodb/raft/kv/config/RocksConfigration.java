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

package io.dingodb.raft.kv.config;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.AbstractWalFilter;
import org.rocksdb.AccessHint;
import org.rocksdb.Cache;
import org.rocksdb.DbPath;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Options;
import org.rocksdb.Statistics;
import org.rocksdb.WALRecoveryMode;

import java.util.List;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class RocksConfigration {

    private Boolean createIfMissing;
    private Boolean createMissingColumnFamilies;
    private Boolean errorIfExists;
    private Boolean paranoidChecks;
    private InfoLogLevel infoLogLevel;
    private Integer maxFileOpeningThreads;
    private Statistics statistics;
    private Boolean useFsync;
    private List<DbPath> dbPaths;
    private String dbLogDir;
    private String walDir;
    private Integer maxSubcompactions;
    private Long maxLogFileSize;
    private Long logFileTimeToRoll;
    private Long keepLogFileNum;
    private Long recycleLogFileNum;
    private Long maxManifestFileSize;
    private Integer tableCacheNumshardbits;
    private Long walTtlSeconds;
    private Long walSizeLimitMB;
    private Long maxWriteBatchGroupSizeBytes;
    private Long manifestPreallocationSize;
    private Boolean useDirectReads;
    private Boolean useDirectIoForFlushAndCompaction;
    private Boolean allowFAllocate;
    private Boolean allowMmapReads;
    private Boolean allowMmapWrites;
    private Boolean isFdCloseOnExec;
    private Boolean adviseRandomOnOpen;
    private Long dbWriteBufferSize;
    private AccessHint accessHintOnCompactionStart;
    private Boolean newTableReaderForCompactionInputs;
    private Long randomAccessMaxBufferSize;
    private Boolean useAdaptiveMutex;
    private List<AbstractEventListener> listeners;
    private Boolean enableThreadTracking;
    private Boolean enablePipelinedWrite;
    private Boolean unorderedWrite;
    private Boolean allowConcurrentMemtableWrite;
    private Boolean enableWriteThreadAdaptiveYield;
    private Long writeThreadMaxYieldUsec;
    private Long writeThreadSlowYieldUsec;
    private Boolean skipStatsUpdateOnDbOpen;
    private Boolean skipCheckingSstFileSizesOnDbOpen;
    private WALRecoveryMode walRecoveryMode;
    private Boolean allow2pc;
    private Cache rowCache;
    private AbstractWalFilter walFilter;
    private Boolean failIfOptionsFileError;
    private Boolean dumpMallocStats;
    private Boolean avoidFlushDuringRecovery;
    private Boolean allowIngestBehind;
    private Boolean preserveDeletes;
    private Boolean twoWriteQueues;
    private Boolean manualWalFlush;
    private Boolean atomicFlush;
    private Boolean avoidUnnecessaryBlockingIO;
    private Boolean persistStatsToDisk;
    private Boolean writeDbidToManifest;
    private Long logReadaheadSize;
    private Boolean bestEffortsRecovery;
    private Integer maxBgErrorResumeCount;
    private Long bgerrorResumeRetryInterval;

    // mutable
    public Integer maxBackgroundJobs;
    public Integer baseBackgroundCompactions;
    public Boolean avoidFlushDuringShutdown;
    public Long writableFileMaxBufferSize;
    public Long delayedWriteRate;
    public Long maxTotalWalSize;
    public Long deleteObsoleteFilesPeriodMicros;
    public Integer statsDumpPeriodSec;
    public Integer statsPersistPeriodSec;
    public Long statsHistoryBufferSize;
    public Integer maxOpenFiles;
    public Long bytesPerSync;
    public Long walBytesPerSync;
    public Boolean strictBytesPerSync;
    public Long compactionReadaheadSize;

    public Options toRocksDBOptions() {
        Options options = new Options();
        if (createIfMissing != null) {
            options.setCreateIfMissing(createIfMissing);
        } else {
            options.setCreateIfMissing(true);
        }
        if (createMissingColumnFamilies != null) {
            options.setCreateMissingColumnFamilies(createMissingColumnFamilies);
        }
        if (errorIfExists != null) {
            options.setErrorIfExists(errorIfExists);
        }
        if (paranoidChecks != null) {
            options.setParanoidChecks(paranoidChecks);
        }
        if (infoLogLevel != null) {
            options.setInfoLogLevel(infoLogLevel);
        }
        if (maxFileOpeningThreads != null) {
            options.setMaxFileOpeningThreads(maxFileOpeningThreads);
        }
        if (statistics != null) {
            options.setStatistics(statistics);
        }
        if (useFsync != null) {
            options.setUseFsync(useFsync);
        }
        if (dbPaths != null) {
            options.setDbPaths(dbPaths);
        }
        if (dbLogDir != null) {
            options.setDbLogDir(dbLogDir);
        }
        if (walDir != null) {
            options.setWalDir(walDir);
        }
        if (maxSubcompactions != null) {
            options.setMaxSubcompactions(maxSubcompactions);
        }
        if (maxLogFileSize != null) {
            options.setMaxLogFileSize(maxLogFileSize);
        }
        if (logFileTimeToRoll != null) {
            options.setLogFileTimeToRoll(logFileTimeToRoll);
        }
        if (keepLogFileNum != null) {
            options.setKeepLogFileNum(keepLogFileNum);
        }
        if (recycleLogFileNum != null) {
            options.setRecycleLogFileNum(recycleLogFileNum);
        }
        if (maxManifestFileSize != null) {
            options.setMaxManifestFileSize(maxManifestFileSize);
        }
        if (tableCacheNumshardbits != null) {
            options.setTableCacheNumshardbits(tableCacheNumshardbits);
        }
        if (walTtlSeconds != null) {
            options.setWalTtlSeconds(walTtlSeconds);
        }
        if (walSizeLimitMB != null) {
            options.setWalSizeLimitMB(walSizeLimitMB);
        }
        if (maxWriteBatchGroupSizeBytes != null) {
            options.setMaxWriteBatchGroupSizeBytes(maxWriteBatchGroupSizeBytes);
        }
        if (manifestPreallocationSize != null) {
            options.setManifestPreallocationSize(manifestPreallocationSize);
        }
        if (useDirectReads != null) {
            options.setUseDirectReads(useDirectReads);
        }
        if (useDirectIoForFlushAndCompaction != null) {
            options.setUseDirectIoForFlushAndCompaction(useDirectIoForFlushAndCompaction);
        }
        if (allowFAllocate != null) {
            options.setAllowFAllocate(allowFAllocate);
        }
        if (allowMmapReads != null) {
            options.setAllowMmapReads(allowMmapReads);
        }
        if (allowMmapWrites != null) {
            options.setAllowMmapWrites(allowMmapWrites);
        }
        if (isFdCloseOnExec != null) {
            options.setIsFdCloseOnExec(isFdCloseOnExec);
        }
        if (adviseRandomOnOpen != null) {
            options.setAdviseRandomOnOpen(adviseRandomOnOpen);
        }
        if (dbWriteBufferSize != null) {
            options.setDbWriteBufferSize(dbWriteBufferSize);
        }
        if (accessHintOnCompactionStart != null) {
            options.setAccessHintOnCompactionStart(accessHintOnCompactionStart);
        }
        if (newTableReaderForCompactionInputs != null) {
            options.setNewTableReaderForCompactionInputs(newTableReaderForCompactionInputs);
        }
        if (randomAccessMaxBufferSize != null) {
            options.setRandomAccessMaxBufferSize(randomAccessMaxBufferSize);
        }
        if (useAdaptiveMutex != null) {
            options.setUseAdaptiveMutex(useAdaptiveMutex);
        }
        if (listeners != null) {
            options.setListeners(listeners);
        }
        if (enableThreadTracking != null) {
            options.setEnableThreadTracking(enableThreadTracking);
        }
        if (enablePipelinedWrite != null) {
            options.setEnablePipelinedWrite(enablePipelinedWrite);
        }
        if (unorderedWrite != null) {
            options.setUnorderedWrite(unorderedWrite);
        }
        if (allowConcurrentMemtableWrite != null) {
            options.setAllowConcurrentMemtableWrite(allowConcurrentMemtableWrite);
        }
        if (enableWriteThreadAdaptiveYield != null) {
            options.setEnableWriteThreadAdaptiveYield(enableWriteThreadAdaptiveYield);
        }
        if (writeThreadMaxYieldUsec != null) {
            options.setWriteThreadMaxYieldUsec(writeThreadMaxYieldUsec);
        }
        if (writeThreadSlowYieldUsec != null) {
            options.setWriteThreadSlowYieldUsec(writeThreadSlowYieldUsec);
        }
        if (skipStatsUpdateOnDbOpen != null) {
            options.setSkipStatsUpdateOnDbOpen(skipStatsUpdateOnDbOpen);
        }
        if (skipCheckingSstFileSizesOnDbOpen != null) {
            options.setSkipCheckingSstFileSizesOnDbOpen(skipCheckingSstFileSizesOnDbOpen);
        }
        if (walRecoveryMode != null) {
            options.setWalRecoveryMode(walRecoveryMode);
        }
        if (allow2pc != null) {
            options.setAllow2pc(allow2pc);
        }
        if (rowCache != null) {
            options.setRowCache(rowCache);
        }
        if (walFilter != null) {
            options.setWalFilter(walFilter);
        }
        if (failIfOptionsFileError != null) {
            options.setFailIfOptionsFileError(failIfOptionsFileError);
        }
        if (dumpMallocStats != null) {
            options.setDumpMallocStats(dumpMallocStats);
        }
        if (avoidFlushDuringRecovery != null) {
            options.setAvoidFlushDuringRecovery(avoidFlushDuringRecovery);
        }
        if (allowIngestBehind != null) {
            options.setAllowIngestBehind(allowIngestBehind);
        }
        if (preserveDeletes != null) {
            options.setPreserveDeletes(preserveDeletes);
        }
        if (twoWriteQueues != null) {
            options.setTwoWriteQueues(twoWriteQueues);
        }
        if (manualWalFlush != null) {
            options.setManualWalFlush(manualWalFlush);
        }
        if (atomicFlush != null) {
            options.setAtomicFlush(atomicFlush);
        }
        if (avoidUnnecessaryBlockingIO != null) {
            options.setAvoidUnnecessaryBlockingIO(avoidUnnecessaryBlockingIO);
        }
        if (persistStatsToDisk != null) {
            options.setPersistStatsToDisk(persistStatsToDisk);
        }
        if (writeDbidToManifest != null) {
            options.setWriteDbidToManifest(writeDbidToManifest);
        }
        if (logReadaheadSize != null) {
            options.setLogReadaheadSize(logReadaheadSize);
        }
        if (bestEffortsRecovery != null) {
            options.setBestEffortsRecovery(bestEffortsRecovery);
        }
        if (maxBgErrorResumeCount != null) {
            options.setMaxBgErrorResumeCount(maxBgErrorResumeCount);
        }
        if (bgerrorResumeRetryInterval != null) {
            options.setBgerrorResumeRetryInterval(bgerrorResumeRetryInterval);
        }
        if (maxBackgroundJobs != null) {
            options.setMaxBackgroundJobs(maxBackgroundJobs);
        }
        if (baseBackgroundCompactions != null) {
            options.setBaseBackgroundCompactions(baseBackgroundCompactions);
        }
        if (avoidFlushDuringShutdown != null) {
            options.setAvoidFlushDuringShutdown(avoidFlushDuringShutdown);
        }
        if (writableFileMaxBufferSize != null) {
            options.setWritableFileMaxBufferSize(writableFileMaxBufferSize);
        }
        if (delayedWriteRate != null) {
            options.setDelayedWriteRate(delayedWriteRate);
        }
        if (maxTotalWalSize != null) {
            options.setMaxTotalWalSize(maxTotalWalSize);
        }
        if (deleteObsoleteFilesPeriodMicros != null) {
            options.setDeleteObsoleteFilesPeriodMicros(deleteObsoleteFilesPeriodMicros);
        }
        if (statsDumpPeriodSec != null) {
            options.setStatsDumpPeriodSec(statsDumpPeriodSec);
        }
        if (statsPersistPeriodSec != null) {
            options.setStatsPersistPeriodSec(statsPersistPeriodSec);
        }
        if (statsHistoryBufferSize != null) {
            options.setStatsHistoryBufferSize(statsHistoryBufferSize);
        }
        if (maxOpenFiles != null) {
            options.setMaxOpenFiles(maxOpenFiles);
        }
        if (bytesPerSync != null) {
            options.setBytesPerSync(bytesPerSync);
        }
        if (walBytesPerSync != null) {
            options.setWalBytesPerSync(walBytesPerSync);
        }
        if (strictBytesPerSync != null) {
            options.setStrictBytesPerSync(strictBytesPerSync);
        }
        if (compactionReadaheadSize != null) {
            options.setCompactionReadaheadSize(compactionReadaheadSize);
        }
        return options;
    }

}
