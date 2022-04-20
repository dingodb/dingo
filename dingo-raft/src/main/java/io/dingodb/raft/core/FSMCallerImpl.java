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

package io.dingodb.raft.core;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.dingodb.raft.Closure;
import io.dingodb.raft.FSMCaller;
import io.dingodb.raft.StateMachine;
import io.dingodb.raft.Status;
import io.dingodb.raft.closure.ClosureQueue;
import io.dingodb.raft.closure.LoadSnapshotClosure;
import io.dingodb.raft.closure.SaveSnapshotClosure;
import io.dingodb.raft.closure.TaskClosure;
import io.dingodb.raft.conf.Configuration;
import io.dingodb.raft.conf.ConfigurationEntry;
import io.dingodb.raft.entity.EnumOutter;
import io.dingodb.raft.entity.LeaderChangeContext;
import io.dingodb.raft.entity.LogEntry;
import io.dingodb.raft.entity.LogId;
import io.dingodb.raft.entity.PeerId;
import io.dingodb.raft.entity.RaftOutter;
import io.dingodb.raft.error.RaftError;
import io.dingodb.raft.error.RaftException;
import io.dingodb.raft.option.FSMCallerOptions;
import io.dingodb.raft.rpc.ReportTarget;
import io.dingodb.raft.storage.LogManager;
import io.dingodb.raft.storage.snapshot.SnapshotReader;
import io.dingodb.raft.storage.snapshot.SnapshotWriter;
import io.dingodb.raft.util.Describer;
import io.dingodb.raft.util.DisruptorBuilder;
import io.dingodb.raft.util.DisruptorMetricSet;
import io.dingodb.raft.util.LogExceptionHandler;
import io.dingodb.raft.util.NamedThreadFactory;
import io.dingodb.raft.util.OnlyForTest;
import io.dingodb.raft.util.Requires;
import io.dingodb.raft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public class FSMCallerImpl implements FSMCaller {
    private static final Logger LOG = LoggerFactory.getLogger(FSMCallerImpl.class);

    /*
     * Task type
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-03 11:12:25 AM.
     */
    private enum TaskType {
        IDLE, //
        COMMITTED, //
        SNAPSHOT_SAVE, //
        SNAPSHOT_LOAD, //
        LEADER_STOP, //
        LEADER_START, //
        START_FOLLOWING, //
        STOP_FOLLOWING, //
        SHUTDOWN, //
        FLUSH, //
        ERROR;

        private String metricName;

        public String metricName() {
            if (this.metricName == null) {
                this.metricName = "fsm-" + name().toLowerCase().replaceAll("_", "-");
            }
            return this.metricName;
        }
    }

    /**
     * Apply task for disruptor.
     */
    private static class ApplyTask {
        TaskType type;
        // union fields
        long committedIndex;
        long term;
        Status status;
        LeaderChangeContext leaderChangeCtx;
        Closure done;
        CountDownLatch shutdownLatch;

        public void reset() {
            this.type = null;
            this.committedIndex = 0;
            this.term = 0;
            this.status = null;
            this.leaderChangeCtx = null;
            this.done = null;
            this.shutdownLatch = null;
        }
    }

    private static class ApplyTaskFactory implements EventFactory<ApplyTask> {

        @Override
        public ApplyTask newInstance() {
            return new ApplyTask();
        }
    }

    private class ApplyTaskHandler implements EventHandler<ApplyTask> {
        // max committed index in current batch, reset to -1 every batch
        private long maxCommittedIndex = -1;

        @Override
        public void onEvent(final ApplyTask event, final long sequence, final boolean endOfBatch) throws Exception {
            this.maxCommittedIndex = runApplyTask(event, this.maxCommittedIndex, endOfBatch);
        }
    }

    private LogManager logManager;
    private StateMachine fsm;
    private ClosureQueue closureQueue;
    private final AtomicLong lastAppliedIndex;
    private long lastAppliedTerm;
    private Closure afterShutdown;
    private NodeImpl node;
    private volatile TaskType currTask;
    private final AtomicLong applyingIndex;
    private volatile RaftException error;
    private Disruptor<ApplyTask> disruptor;
    private RingBuffer<ApplyTask> taskQueue;
    private volatile CountDownLatch shutdownLatch;
    private NodeMetrics nodeMetrics;
    private final CopyOnWriteArrayList<LastAppliedLogIndexListener> lastAppliedLogIndexListeners
        = new CopyOnWriteArrayList<>();
    private final AtomicLong snapshotAppliedIndex;

    public FSMCallerImpl() {
        super();
        this.currTask = TaskType.IDLE;
        this.lastAppliedIndex = new AtomicLong(0);
        this.applyingIndex = new AtomicLong(0);
        this.snapshotAppliedIndex = new AtomicLong(0);
    }

    @Override
    public boolean init(final FSMCallerOptions opts) {
        this.logManager = opts.getLogManager();
        this.fsm = opts.getFsm();
        this.closureQueue = opts.getClosureQueue();
        this.afterShutdown = opts.getAfterShutdown();
        this.node = opts.getNode();
        this.nodeMetrics = this.node.getNodeMetrics();
        this.lastAppliedIndex.set(opts.getBootstrapId().getIndex());
        notifyLastAppliedIndexUpdated(this.lastAppliedIndex.get());
        this.lastAppliedTerm = opts.getBootstrapId().getTerm();
        this.disruptor = DisruptorBuilder.<ApplyTask>newInstance() //
            .setEventFactory(new ApplyTaskFactory()) //
            .setRingBufferSize(opts.getDisruptorBufferSize()) //
            .setThreadFactory(new NamedThreadFactory("JRaft-FSMCaller-Disruptor-", true)) //
            .setProducerType(ProducerType.MULTI) //
            .setWaitStrategy(new BlockingWaitStrategy()) //
            .build();
        this.disruptor.handleEventsWith(new ApplyTaskHandler());
        this.disruptor.setDefaultExceptionHandler(new LogExceptionHandler<Object>(getClass().getSimpleName()));
        this.taskQueue = this.disruptor.start();
        if (this.nodeMetrics.getMetricRegistry() != null) {
            this.nodeMetrics.getMetricRegistry().register("jraft-fsm-caller-disruptor",
                new DisruptorMetricSet(this.taskQueue));
        }
        this.error = new RaftException(EnumOutter.ErrorType.ERROR_TYPE_NONE);
        LOG.info("Starts FSMCaller successfully.");
        return true;
    }

    @Override
    public synchronized void shutdown() {
        if (this.shutdownLatch != null) {
            return;
        }
        LOG.info("Shutting down FSMCaller...");

        if (this.taskQueue != null) {
            final CountDownLatch latch = new CountDownLatch(1);
            this.shutdownLatch = latch;
            Utils.runInThread(() -> this.taskQueue.publishEvent((task, sequence) -> {
                task.reset();
                task.type = TaskType.SHUTDOWN;
                task.shutdownLatch = latch;
            }));
        }
        doShutdown();
    }

    @Override
    public void addLastAppliedLogIndexListener(final LastAppliedLogIndexListener listener) {
        this.lastAppliedLogIndexListeners.add(listener);
    }

    private boolean enqueueTask(final EventTranslator<ApplyTask> tpl) {
        if (this.shutdownLatch != null) {
            // Shutting down
            LOG.warn("FSMCaller is stopped, can not apply new task.");
            return false;
        }

        // publish Event(if the queue is full, this operation will block)
        this.taskQueue.publishEvent(tpl);
        return true;

        /*
        if (!this.taskQueue.tryPublishEvent(tpl)) {
            setError(new RaftException(EnumOutter.ErrorType.ERROR_TYPE_STATE_MACHINE, new Status(RaftError.EBUSY,
                "FSMCaller is overload.")));
            return false;
        }
        return true;
         */
    }

    @Override
    public boolean onCommitted(final long committedIndex) {
        return enqueueTask((task, sequence) -> {
            task.type = TaskType.COMMITTED;
            task.committedIndex = committedIndex;
        });
    }

    /**
     * Flush all events in disruptor.
     */
    @OnlyForTest
    void flush() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        enqueueTask((task, sequence) -> {
            task.type = TaskType.FLUSH;
            task.shutdownLatch = latch;
        });
        latch.await();
    }

    @Override
    public boolean onSnapshotLoad(final LoadSnapshotClosure done) {
        return enqueueTask((task, sequence) -> {
            task.type = TaskType.SNAPSHOT_LOAD;
            task.done = done;
        });
    }

    @Override
    public boolean onSnapshotSave(final SaveSnapshotClosure done) {
        return enqueueTask((task, sequence) -> {
            task.type = TaskType.SNAPSHOT_SAVE;
            task.done = done;
        });
    }

    @Override
    public boolean onLeaderStop(final Status status) {
        return enqueueTask((task, sequence) -> {
            task.type = TaskType.LEADER_STOP;
            task.status = new Status(status);
        });
    }

    @Override
    public boolean onLeaderStart(final long term) {
        return enqueueTask((task, sequence) -> {
            task.type = TaskType.LEADER_START;
            task.term = term;
        });
    }

    @Override
    public boolean onStartFollowing(final LeaderChangeContext ctx) {
        return enqueueTask((task, sequence) -> {
            task.type = TaskType.START_FOLLOWING;
            task.leaderChangeCtx = new LeaderChangeContext(ctx.getLeaderId(), ctx.getTerm(), ctx.getStatus());
        });
    }

    @Override
    public boolean onStopFollowing(final LeaderChangeContext ctx) {
        return enqueueTask((task, sequence) -> {
            task.type = TaskType.STOP_FOLLOWING;
            task.leaderChangeCtx = new LeaderChangeContext(ctx.getLeaderId(), ctx.getTerm(), ctx.getStatus());
        });
    }

    /*
     * Closure runs with an error.
     * @author boyan (boyan@alibaba-inc.com)
     *
     * 2018-Apr-04 2:20:31 PM
     */
    public class OnErrorClosure implements Closure {
        private RaftException error;

        public OnErrorClosure(final RaftException error) {
            super();
            this.error = error;
        }

        public RaftException getError() {
            return this.error;
        }

        public void setError(final RaftException error) {
            this.error = error;
        }

        @Override
        public void run(final Status st) {
        }
    }

    @Override
    public boolean onError(final RaftException error) {
        if (!this.error.getStatus().isOk()) {
            LOG.warn("FSMCaller already in error status, ignore new error.", error);
            return false;
        }
        final OnErrorClosure c = new OnErrorClosure(error);
        return enqueueTask((task, sequence) -> {
            task.type = TaskType.ERROR;
            task.done = c;
        });
    }

    @Override
    public long getLastAppliedIndex() {
        return this.lastAppliedIndex.get();
    }

    @Override
    public synchronized void join() throws InterruptedException {
        if (this.shutdownLatch != null) {
            this.shutdownLatch.await();
            this.disruptor.shutdown();
            if (this.afterShutdown != null) {
                this.afterShutdown.run(Status.OK());
                this.afterShutdown = null;
            }
            this.shutdownLatch = null;
        }
    }

    @SuppressWarnings("ConstantConditions")
    private long runApplyTask(final ApplyTask task, long maxCommittedIndex, final boolean endOfBatch) {
        CountDownLatch shutdown = null;
        if (task.type == TaskType.COMMITTED) {
            if (task.committedIndex > maxCommittedIndex) {
                maxCommittedIndex = task.committedIndex;
            }
            task.reset();
        } else {
            if (maxCommittedIndex >= 0) {
                this.currTask = TaskType.COMMITTED;
                doCommitted(maxCommittedIndex);
                maxCommittedIndex = -1L; // reset maxCommittedIndex
            }
            final long startMs = Utils.monotonicMs();
            try {
                switch (task.type) {
                    case COMMITTED:
                        Requires.requireTrue(false, "Impossible");
                        break;
                    case SNAPSHOT_SAVE:
                        this.currTask = TaskType.SNAPSHOT_SAVE;
                        if (passByStatus(task.done)) {
                            doSnapshotSave((SaveSnapshotClosure) task.done, null);
                        }
                        break;
                    case SNAPSHOT_LOAD:
                        this.currTask = TaskType.SNAPSHOT_LOAD;
                        if (passByStatus(task.done)) {
                            doSnapshotLoad((LoadSnapshotClosure) task.done);
                        }
                        break;
                    case LEADER_STOP:
                        this.currTask = TaskType.LEADER_STOP;
                        doLeaderStop(task.status);
                        break;
                    case LEADER_START:
                        this.currTask = TaskType.LEADER_START;
                        doLeaderStart(task.term);
                        break;
                    case START_FOLLOWING:
                        this.currTask = TaskType.START_FOLLOWING;
                        doStartFollowing(task.leaderChangeCtx);
                        break;
                    case STOP_FOLLOWING:
                        this.currTask = TaskType.STOP_FOLLOWING;
                        doStopFollowing(task.leaderChangeCtx);
                        break;
                    case ERROR:
                        this.currTask = TaskType.ERROR;
                        doOnError((OnErrorClosure) task.done);
                        break;
                    case IDLE:
                        Requires.requireTrue(false, "Can't reach here");
                        break;
                    case SHUTDOWN:
                        this.currTask = TaskType.SHUTDOWN;
                        shutdown = task.shutdownLatch;
                        break;
                    case FLUSH:
                        this.currTask = TaskType.FLUSH;
                        shutdown = task.shutdownLatch;
                        break;
                    default:
                        break;
                }
            } finally {
                this.nodeMetrics.recordLatency(task.type.metricName(), Utils.monotonicMs() - startMs);
                task.reset();
            }
        }
        try {
            if (endOfBatch && maxCommittedIndex >= 0) {
                this.currTask = TaskType.COMMITTED;
                doCommitted(maxCommittedIndex);
                maxCommittedIndex = -1L; // reset maxCommittedIndex
            }
            this.currTask = TaskType.IDLE;
            return maxCommittedIndex;
        } finally {
            if (shutdown != null) {
                shutdown.countDown();
            }
        }
    }

    private void doShutdown() {
        if (this.node != null) {
            this.node = null;
        }
        if (this.fsm != null) {
            this.fsm.onShutdown();
        }
    }

    private void notifyLastAppliedIndexUpdated(final long lastAppliedIndex) {
        for (final LastAppliedLogIndexListener listener : this.lastAppliedLogIndexListeners) {
            listener.onApplied(lastAppliedIndex);
        }
    }

    private void doCommitted(final long committedIndex) {
        if (!this.error.getStatus().isOk()) {
            return;
        }
        final long lastAppliedIndex = this.lastAppliedIndex.get();
        // We can tolerate the disorder of committed_index
        if (lastAppliedIndex >= committedIndex) {
            return;
        }
        final long startMs = Utils.monotonicMs();
        try {
            final List<Closure> closures = new ArrayList<>();
            final List<TaskClosure> taskClosures = new ArrayList<>();
            final long firstClosureIndex = this.closureQueue.popClosureUntil(committedIndex, closures, taskClosures);

            // Calls TaskClosure#onCommitted if necessary
            onTaskCommitted(taskClosures);

            Requires.requireTrue(firstClosureIndex >= 0, "Invalid firstClosureIndex");
            IteratorImpl iterImpl =
                new IteratorImpl(this.fsm, this.logManager, closures, firstClosureIndex,
                lastAppliedIndex, committedIndex, this.applyingIndex);

            boolean doSnapshotByIndex = false;
            while (iterImpl.isGood()) {
                final LogEntry logEntry = iterImpl.entry();
                if (logEntry.getType() != EnumOutter.EntryType.ENTRY_TYPE_DATA) {
                    if (logEntry.getType() == EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION) {
                        if (logEntry.getOldPeers() != null && !logEntry.getOldPeers().isEmpty()) {
                            // Joint stage is not supposed to be noticeable by end users.
                            this.fsm.onConfigurationCommitted(new Configuration(iterImpl.entry().getPeers()));
                        }
                    } else if (logEntry.getType() == EnumOutter.EntryType.ENTRY_TYPE_MSG) {
                        LOG.info("FSMCallerImpl doCommitted, data has remainning: {}, remaining: {}"
                                + ", position: {}.",
                            logEntry.getData().hasRemaining(),
                            logEntry.getData().remaining(),
                            logEntry.getData().position());

                        int subType = logEntry.getMsgSubType();
                        if (subType == LogEntry.SUB_TYPE_FREEZE_SNAPSHOT) {
                            LOG.info("FSMCallerImpl doCommitted, sub type: {}",
                                LogEntry.subTypeToString(subType));
                            node.doFreezeSnapshot();
                        } else if (subType == LogEntry.SUB_TYPE_SNAPSHOT_BY_INDEX) {
                            LOG.info("FSMCallerImpl doCommitted, sub type: {}",
                                LogEntry.subTypeToString(subType));
                            doSnapshotByIndex = true;
                        } else {
                            LOG.error("FSMCallerImpl doCommitted, unexpected sub type: {}",
                                LogEntry.subTypeToString(subType));
                        }
                    }

                    if (iterImpl.done() != null) {
                        // For other entries, we have nothing to do besides flush the
                        // pending tasks and run this closure to notify the caller that the
                        // entries before this one were successfully committed and applied.
                        iterImpl.done().run(Status.OK());
                        /*if (logEntry.getType() == EnumOutter.EntryType.ENTRY_TYPE_MSG) {
                            LOG.info("FSMCallerImpl doCommitted type msg status OK, type: {}, sub type: {}",
                                logEntry.getType().name(), LogEntry.subTypeToString(logEntry.getMsgSubType()));
                        }*/
                    }
                    if (doSnapshotByIndex) {
                        long currIndex = iterImpl.getIndex();
                        iterImpl = new IteratorImpl(this.fsm, this.logManager, closures, firstClosureIndex,
                            currIndex + 1, currIndex, this.applyingIndex);
                        break;
                    }
                    iterImpl.next();
                    continue;
                }

                // Apply data task to user state machine
                doApplyTasks(iterImpl);
            }

            if (iterImpl.hasError()) {
                setError(iterImpl.getError());
                iterImpl.runTheRestClosureWithError();
            }
            final long lastIndex = iterImpl.getIndex() - 1;
            final long lastTerm = this.logManager.getTerm(lastIndex);
            final LogId lastAppliedId = new LogId(lastIndex, lastTerm);
            this.lastAppliedIndex.set(lastIndex);
            this.lastAppliedTerm = lastTerm;
            this.logManager.setAppliedId(lastAppliedId);
            notifyLastAppliedIndexUpdated(lastIndex);
            if (doSnapshotByIndex) {
                this.snapshotAppliedIndex.set(lastIndex);
                LOG.info("FSMCallerImpl doSnapshotByAppliedIndex, lastAppliedTerm: {}, lastAppliedIndex: {}.",
                    this.lastAppliedTerm, this.lastAppliedIndex);
                node.snapshotByAppliedIndex();
            }
        } finally {
            this.nodeMetrics.recordLatency("fsm-commit", Utils.monotonicMs() - startMs);
        }
    }

    private void onTaskCommitted(final List<TaskClosure> closures) {
        for (TaskClosure done : closures) {
            done.onCommitted();
        }
    }

    private void doApplyTasks(final IteratorImpl iterImpl) {
        final IteratorWrapper iter = new IteratorWrapper(iterImpl);
        final long startApplyMs = Utils.monotonicMs();
        final long startIndex = iter.getIndex();
        try {
            this.fsm.onApply(iter);
        } finally {
            this.nodeMetrics.recordLatency("fsm-apply-tasks", Utils.monotonicMs() - startApplyMs);
            this.nodeMetrics.recordSize("fsm-apply-tasks-count", iter.getIndex() - startIndex);
        }
        if (iter.hasNext()) {
            LOG.error("Iterator is still valid, did you return before iterator reached the end?");
        }
        // Try move to next in case that we pass the same log twice.
        iter.next();
    }

    private void doSnapshotSave(final SaveSnapshotClosure done, ReportTarget reportTarget) {
        Requires.requireNonNull(done, "SaveSnapshotClosure is null");
        final long lastAppliedIndex = this.lastAppliedIndex.get();
        final RaftOutter.SnapshotMeta.Builder metaBuilder = RaftOutter.SnapshotMeta.newBuilder() //
            .setLastIncludedIndex(lastAppliedIndex) //
            .setLastIncludedTerm(this.lastAppliedTerm);
        final ConfigurationEntry confEntry = this.logManager.getConfiguration(lastAppliedIndex);
        if (confEntry == null || confEntry.isEmpty()) {
            LOG.error("Empty conf entry for lastAppliedIndex={}", lastAppliedIndex);
            Utils.runClosureInThread(done, new Status(RaftError.EINVAL, "Empty conf entry for lastAppliedIndex=%s",
                lastAppliedIndex));
            return;
        }
        for (final PeerId peer : confEntry.getConf()) {
            metaBuilder.addPeers(peer.toString());
        }
        for (final PeerId peer : confEntry.getConf().getLearners()) {
            metaBuilder.addLearners(peer.toString());
        }
        if (confEntry.getOldConf() != null) {
            for (final PeerId peer : confEntry.getOldConf()) {
                metaBuilder.addOldPeers(peer.toString());
            }
            for (final PeerId peer : confEntry.getOldConf().getLearners()) {
                metaBuilder.addOldLearners(peer.toString());
            }
        }
        final SnapshotWriter writer = done.start(metaBuilder.build());
        if (writer == null) {
            done.run(new Status(RaftError.EINVAL, "snapshot_storage create SnapshotWriter failed"));
            return;
        }
        if (reportTarget != null) {
            reportTarget.setLastAppliedIndex(lastAppliedIndex);
            if (lastAppliedIndex != this.snapshotAppliedIndex.get()) {
                LOG.warn("FSMCallerImpl doSnapshotSave, snapshot index not match,"
                    + "snapshotAppliedIndex: {}, lastAppliedIndex: {}.", this.snapshotAppliedIndex, lastAppliedIndex);
            }
        }
        this.fsm.onSnapshotSave(writer, done, reportTarget);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StateMachine [");
        switch (this.currTask) {
            case IDLE:
                sb.append("Idle");
                break;
            case COMMITTED:
                sb.append("Applying logIndex=").append(this.applyingIndex);
                break;
            case SNAPSHOT_SAVE:
                sb.append("Saving snapshot");
                break;
            case SNAPSHOT_LOAD:
                sb.append("Loading snapshot");
                break;
            case ERROR:
                sb.append("Notifying error");
                break;
            case LEADER_STOP:
                sb.append("Notifying leader stop");
                break;
            case LEADER_START:
                sb.append("Notifying leader start");
                break;
            case START_FOLLOWING:
                sb.append("Notifying start following");
                break;
            case STOP_FOLLOWING:
                sb.append("Notifying stop following");
                break;
            case SHUTDOWN:
                sb.append("Shutting down");
                break;
            default:
                break;
        }
        return sb.append(']').toString();
    }

    private void doSnapshotLoad(final LoadSnapshotClosure done) {
        Requires.requireNonNull(done, "LoadSnapshotClosure is null");
        final SnapshotReader reader = done.start();
        if (reader == null) {
            done.run(new Status(RaftError.EINVAL, "open SnapshotReader failed"));
            return;
        }
        final RaftOutter.SnapshotMeta meta = reader.load();
        if (meta == null) {
            done.run(new Status(RaftError.EINVAL, "SnapshotReader load meta failed"));
            if (reader.getRaftError() == RaftError.EIO) {
                final RaftException err = new RaftException(EnumOutter.ErrorType.ERROR_TYPE_SNAPSHOT, RaftError.EIO,
                    "Fail to load snapshot meta");
                setError(err);
            }
            return;
        }
        final LogId lastAppliedId = new LogId(this.lastAppliedIndex.get(), this.lastAppliedTerm);
        final LogId snapshotId = new LogId(meta.getLastIncludedIndex(), meta.getLastIncludedTerm());
        if (lastAppliedId.compareTo(snapshotId) > 0) {
            done.run(new Status(
                RaftError.ESTALE,
                "Loading a stale snapshot last_applied_index=%d "
                    + "last_applied_term=%d snapshot_index=%d snapshot_term=%d",
                lastAppliedId.getIndex(), lastAppliedId.getTerm(), snapshotId.getIndex(), snapshotId.getTerm()));
            return;
        }
        if (!this.fsm.onSnapshotLoad(reader)) {
            done.run(new Status(-1, "StateMachine onSnapshotLoad failed"));
            final RaftException e = new RaftException(EnumOutter.ErrorType.ERROR_TYPE_STATE_MACHINE,
                RaftError.ESTATEMACHINE, "StateMachine onSnapshotLoad failed");
            setError(e);
            return;
        }
        if (meta.getOldPeersCount() == 0) {
            // Joint stage is not supposed to be noticeable by end users.
            final Configuration conf = new Configuration();
            for (int i = 0, size = meta.getPeersCount(); i < size; i++) {
                final PeerId peer = new PeerId();
                Requires.requireTrue(peer.parse(meta.getPeers(i)), "Parse peer failed");
                conf.addPeer(peer);
            }
            this.fsm.onConfigurationCommitted(conf);
        }
        this.lastAppliedIndex.set(meta.getLastIncludedIndex());
        this.lastAppliedTerm = meta.getLastIncludedTerm();
        done.run(Status.OK());
    }

    private void doOnError(final OnErrorClosure done) {
        setError(done.getError());
    }

    private void doLeaderStop(final Status status) {
        this.fsm.onLeaderStop(status);
    }

    private void doLeaderStart(final long term) {
        this.fsm.onLeaderStart(term);
    }

    private void doStartFollowing(final LeaderChangeContext ctx) {
        this.fsm.onStartFollowing(ctx);
    }

    private void doStopFollowing(final LeaderChangeContext ctx) {
        this.fsm.onStopFollowing(ctx);
    }

    private void setError(final RaftException exception) {
        if (this.error.getType() != EnumOutter.ErrorType.ERROR_TYPE_NONE) {
            // already report
            return;
        }
        this.error = exception;
        if (this.fsm != null) {
            this.fsm.onError(exception);
        }
        if (this.node != null) {
            this.node.onError(exception);
        }
    }

    @OnlyForTest
    RaftException getError() {
        return this.error;
    }

    private boolean passByStatus(final Closure done) {
        final Status status = this.error.getStatus();
        if (!status.isOk()) {
            if (done != null) {
                done.run(new Status(RaftError.EINVAL, "FSMCaller is in bad status=`%s`", status));
                return false;
            }
        }
        return true;
    }

    @Override
    public void doSnapshotSaveByAppliedIndex(SaveSnapshotClosure done) {
        LOG.info("FSMCallerImpl doSnapshotSaveByAppliedIndex, lastAppliedTerm: {}, lastAppliedIndex: {}.",
            this.lastAppliedTerm, this.lastAppliedIndex);

        if (!passByStatus(done)) {
            LOG.error("FSMCallerImpl doSnapshotSaveByAppliedIndex, status not OK, "
                    + "lastAppliedTerm: {}, lastAppliedIndex: {}.",
                this.lastAppliedTerm, this.lastAppliedIndex);
            return;
        }

        ReportTarget reportTarget = new ReportTarget();
        reportTarget.setIp(node.getLeaderId().getIp());
        reportTarget.setPort(node.getOptions().getServerExchangePort());
        doSnapshotSave(done, reportTarget);
    }

    @Override
    public void reportFreezeSnapshotResult(final boolean freezeResult, final String errMsg) {
        LOG.info("reportFreezeSnapshotResult, freezeResult: {}, errMsg: {}.", freezeResult, errMsg);
        ReportTarget reportTarget = new ReportTarget();
        reportTarget.setIp(node.getLeaderId().getIp());
        reportTarget.setPort(node.getOptions().getServerExchangePort());
        this.fsm.onReportFreezeSnapshotResult(freezeResult, errMsg, reportTarget);
        this.node.restartUnfreezingSnapshotTimer();
    }

    @Override
    public void describe(final Describer.Printer out) {
        out.print("  ") //
            .println(toString());
    }
}
