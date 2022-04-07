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

import io.dingodb.raft.Closure;
import io.dingodb.raft.StateMachine;
import io.dingodb.raft.Status;
import io.dingodb.raft.conf.Configuration;
import io.dingodb.raft.entity.LeaderChangeContext;
import io.dingodb.raft.error.RaftException;
import io.dingodb.raft.rpc.ReportTarget;
import io.dingodb.raft.storage.snapshot.SnapshotReader;
import io.dingodb.raft.storage.snapshot.SnapshotWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Refer to SOFAJRaft: <A>https://github.com/sofastack/sofa-jraft/<A/>
public abstract class StateMachineAdapter implements StateMachine {
    private static final Logger LOG = LoggerFactory.getLogger(StateMachineAdapter.class);

    @Override
    public void onShutdown() {
        LOG.info("onShutdown.");
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done) {
        error("onSnapshotSave");
        runClosure(done, "onSnapshotSave");
    }

    @Override
    public void onSnapshotSave(final SnapshotWriter writer, final Closure done, ReportTarget reportTarget) {
        error("onSnapshotSave");
        runClosure(done, "onSnapshotSave");
    }

    @Override
    public boolean onSnapshotLoad(final SnapshotReader reader) {
        error("onSnapshotLoad", "while a snapshot is saved in " + reader.getPath());
        return false;
    }

    @Override
    public void onLeaderStart(final long term) {
        LOG.info("onLeaderStart: term={}.", term);
    }

    @Override
    public void onLeaderStop(final Status status) {
        LOG.info("onLeaderStop: status={}.", status);
    }

    @Override
    public void onError(final RaftException e) {
        LOG.error(
            "Encountered an error={} on StateMachine {}, it's highly recommended to implement this method as raft stops working since some error occurs, you should figure out the cause and repair or remove this node.",
            e.getStatus(), getClassName(), e);
    }

    @Override
    public void onConfigurationCommitted(final Configuration conf) {
        LOG.info("onConfigurationCommitted: {}.", conf);
    }

    @Override
    public void onStopFollowing(final LeaderChangeContext ctx) {
        LOG.info("onStopFollowing: {}.", ctx);
    }

    @Override
    public void onStartFollowing(final LeaderChangeContext ctx) {
        LOG.info("onStartFollowing: {}.", ctx);
    }

    @Override
    public void onReportFreezeSnapshotResult(final boolean freezeResult, final String errMsg,
                                             ReportTarget reportTarget) {
        LOG.info("StateMachineAdapter onReportFreezeSnapshotResult, freezeResult: {}, errMsg: {}" +
                ", reportTarget: {}.", freezeResult, errMsg, reportTarget);
    }

    @Override
    public String getRegionId() {
        LOG.info("StateMachineAdapter getRegionId.");
        return  "";
    }

    @SuppressWarnings("SameParameterValue")
    private void runClosure(final Closure done, final String methodName) {
        done.run(new Status(-1, "%s doesn't implement %s", getClassName(), methodName));
    }

    private String getClassName() {
        return getClass().getName();
    }

    @SuppressWarnings("SameParameterValue")
    private void error(final String methodName) {
        error(methodName, "");
    }

    private void error(final String methodName, final String msg) {
        LOG.error("{} doesn't implement {} {}.", getClassName(), methodName, msg);
    }
}
