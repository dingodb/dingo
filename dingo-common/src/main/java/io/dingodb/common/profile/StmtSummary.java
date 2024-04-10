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

package io.dingodb.common.profile;

import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class StmtSummary {
    public final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private String instance;
    private String schema;
    private String sql;
    private String type;
    private String statementType;
    private String simpleUser;
    private int execCount;
    private long sumLatency;
    private long maxLatency;
    private long minLatency;
    private long avgLatency;
    private long firstSeen;
    private long lastSeen;
    private long sumPlanLatency;
    private long maxPlanLatency;
    private long avgPlanLatency;
    private long sumParseLatency;
    private long maxParseLatency;
    private long avgParseLatency;
    private long sumValidateLatency;
    private long maxValidateLatency;
    private long avgValidateLatency;
    private long sumOptimizeLatency;
    private long maxOptimizeLatency;
    private long avgOptimizeLatency;
    private long sumLockLatency;
    private long maxLockLatency;
    private long avgLockLatency;
    private long sumJobLatency;
    private long maxJobLatency;
    private long avgJobLatency;
    private long sumKvLatency;
    private long maxKvLatency;
    private long avgKvLatency;
    private long sumOperatorLatency;
    private long maxOperatorLatency;
    private long avgOperatorLatency;
    private long sumCommitLatency;
    private long maxCommitLatency;
    private long avgCommitLatency;
    private long sumPreWriteLatency;
    private long maxPreWriteLatency;
    private long avgPreWriteLatency;
    private long sumCleanLatency;
    private long maxCleanLatency;
    private long avgCleanLatency;
    private long sumResultCount;
    private long maxResultCount;
    private long minResultCount;
    private long avgResultCount;
    private long sumAffectedRows;
    private long maxAffectedRows;
    private long avgAffectedRows;
    private long planInCache;
    private String plan;
    private boolean prepared;

    public StmtSummary(String stmtSummaryKey) {
        this.firstSeen = System.currentTimeMillis();
        this.sql = stmtSummaryKey;
        int sqlPos;
        if ((sqlPos = stmtSummaryKey.indexOf(":")) > 0) {
            String prefix = stmtSummaryKey.substring(0, sqlPos);
            if (prefix.startsWith("statement")) {
                this.type = "statement";
                this.schema = prefix.substring(9);
            } else if (prefix.startsWith("prepare")) {
                this.type = "prepare";
                this.schema = prefix.substring(7);
            } else if (prefix.startsWith("executePrepare")) {
                this.type = "executePrepare";
                this.schema = prefix.substring(14);
            }
            this.sql = stmtSummaryKey.substring(sqlPos + 1);
        }
    }

    public void addSqlProfile(SqlProfile profile) {
        lock.writeLock().lock();
        execCount++;
        sumLatency += profile.getDuration();
        if (maxLatency < profile.getDuration()) {
            this.maxLatency = profile.getDuration();
        }
        if (minLatency > profile.getDuration()) {
            this.minLatency = profile.getDuration();
        }
        this.avgLatency = sumLatency / execCount;
        this.instance = profile.getInstance();
        this.simpleUser = profile.getSimpleUser();
        if (profile.getStart() < firstSeen) {
            this.firstSeen = profile.start;
        }
        if (profile.getEnd() > lastSeen) {
            this.lastSeen = profile.getEnd();
        }
        PlanProfile planProfile = profile.getPlanProfile();
        if (planProfile != null) {
            this.sumPlanLatency += planProfile.duration;
            if (this.maxPlanLatency < planProfile.duration) {
                this.maxPlanLatency = planProfile.duration;
            }
            this.avgPlanLatency = sumPlanLatency / execCount;
            this.sumParseLatency += planProfile.getParse();
            if (this.maxParseLatency < planProfile.getParse()) {
                this.maxParseLatency = planProfile.getParse();
            }
            this.avgParseLatency = sumParseLatency / execCount;
            this.sumValidateLatency += planProfile.getValidate();
            if (this.maxValidateLatency < planProfile.getValidate()) {
                this.maxValidateLatency = planProfile.getValidate();
            }
            this.avgValidateLatency = sumValidateLatency / execCount;

            this.sumOptimizeLatency += planProfile.getOptimize();
            if (this.maxOptimizeLatency < planProfile.getOptimize()) {
                this.maxOptimizeLatency = planProfile.getOptimize();
            }
            this.avgOptimizeLatency = sumOptimizeLatency / execCount;
            this.sumLockLatency += planProfile.getLock();
            if (this.maxLockLatency < planProfile.getLock()) {
                this.maxLockLatency = planProfile.getLock();
            }
            if (planProfile.isHitCache()) {
                this.planInCache++;
            }
        }
        this.statementType = profile.getStatementType();

        ExecProfile jobProfile = profile.getExecProfile();
        if (jobProfile != null) {
            this.sumJobLatency += jobProfile.duration;
            if (this.maxJobLatency < jobProfile.duration) {
                this.maxJobLatency = jobProfile.duration;
            }
            this.avgJobLatency = sumJobLatency / execCount;
            // foreach profile to calculate avgKvLatency and set
            // foreach profile to calculate operator duration

            if ("select".equals(statementType)) {
                this.sumResultCount += jobProfile.count;
                if (this.maxResultCount < jobProfile.count) {
                    this.maxResultCount = jobProfile.count;
                }
                if (this.minResultCount > jobProfile.count) {
                    this.minResultCount = jobProfile.count;
                }
                this.avgResultCount = sumResultCount / execCount;
            }
            if ("delete".equals(statementType)
                  || "update".equals(statementType)
                  || "insert".equals(statementType)
                  || "is_dml".equals(statementType)) {
                if (jobProfile.getLastTuple() != null && jobProfile.getLastTuple().length > 0
                    && jobProfile.getLastTuple()[0] instanceof Long) {
                    long affectRows = (long) jobProfile.getLastTuple()[0];
                    this.sumAffectedRows += affectRows;
                    if (this.maxAffectedRows < affectRows) {
                        this.maxAffectedRows = affectRows;
                    }
                    this.avgAffectedRows = sumAffectedRows / execCount;
                }
            }
        }

        CommitProfile commitProfile = profile.getCommitProfile();
        if (commitProfile != null) {
            this.sumCommitLatency += commitProfile.duration;
            if (this.maxCommitLatency < commitProfile.getDuration()) {
                this.maxCommitLatency = commitProfile.getDuration();
            }
            this.avgCommitLatency = this.sumCommitLatency / execCount;

            long preWrite = commitProfile.getPreWrite();
            long commit = commitProfile.getCommit();
            this.sumPreWriteLatency += preWrite;
            if (this.maxPreWriteLatency < preWrite) {
                this.maxPreWriteLatency = preWrite;
            }
            this.avgPreWriteLatency = sumPreWriteLatency / execCount;
            this.sumCommitLatency += commit;
            if (this.maxCommitLatency < commit) {
                this.maxCommitLatency = commit;
            }
            this.avgCommitLatency = sumCommitLatency / execCount;
            this.sumCleanLatency += commitProfile.getClean();
            if (this.maxCleanLatency < commitProfile.getClean()) {
                this.maxCleanLatency = commitProfile.getClean();
            }
            this.avgCleanLatency = sumCleanLatency / execCount;
        }

        this.prepared = profile.isPrepared();
        lock.writeLock().unlock();
    }

    public Object[] getTuple() {
        try {
            lock.readLock().lock();
            return new Object[]{
                schema, sql, instance, statementType, type, simpleUser, execCount, sumLatency, maxLatency, minLatency,
                avgLatency, getTime(firstSeen), getTime(lastSeen), sumPlanLatency, maxPlanLatency, avgPlanLatency,
                sumParseLatency,
                maxParseLatency, avgParseLatency, sumValidateLatency, maxValidateLatency, avgValidateLatency,
                sumOptimizeLatency, maxOptimizeLatency, avgOptimizeLatency, sumLockLatency, maxLockLatency,
                avgLockLatency, sumJobLatency, maxJobLatency, avgJobLatency, sumKvLatency, maxKvLatency,
                avgKvLatency, sumOperatorLatency, maxOperatorLatency, avgOperatorLatency, sumCommitLatency,
                maxCommitLatency, avgCommitLatency, sumPreWriteLatency, maxPreWriteLatency, avgPreWriteLatency,
                sumCleanLatency, maxCleanLatency, avgCleanLatency, sumResultCount, maxResultCount,
                avgResultCount, sumAffectedRows, maxAffectedRows,
                avgAffectedRows, planInCache,
                plan, prepared};
        } finally {
            lock.readLock().unlock();
        }
    }

    public static String getTime(long milliSeconds) {
        DateFormat dtf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return dtf.format(new Timestamp(milliSeconds));
    }
}
