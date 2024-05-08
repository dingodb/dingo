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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class StmtSummary {
    public final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    @Getter
    private List<String> tableList;
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
    private String plan;
    private long planInCache;
    private boolean prepared;
    private String binaryPlan;
    private final String id;

    private long analyzeInc;
    private String state;
    private String msg;

    public StmtSummary(String stmtSummaryKey) {
        this.firstSeen = System.currentTimeMillis();
        this.sql = stmtSummaryKey;
        this.id = UUID.randomUUID().toString().replace("-", "");
        int userPos;
        if ((userPos = stmtSummaryKey.indexOf(">")) > 0) {
            simpleUser = stmtSummaryKey.substring(0, userPos);
            stmtSummaryKey = stmtSummaryKey.substring(userPos + 1);
        }
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
        tableList = new ArrayList<>();
    }

    public void addSqlProfile(SqlProfile profile) {
        lock.writeLock().lock();
        execCount++;
        sumLatency += profile.getDuration();
        if (maxLatency < profile.getDuration()) {
            this.maxLatency = profile.getDuration();
        }
        if (minLatency > profile.getDuration() || minLatency == 0) {
            this.minLatency = profile.getDuration();
        }
        this.avgLatency = sumLatency / execCount;
        this.instance = profile.getInstance();
        this.simpleUser = profile.getSimpleUser();
        this.state = profile.getState();
        this.msg = profile.getMsg();
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
            this.avgLockLatency = sumLockLatency / execCount;
            if (planProfile.isHitCache()) {
                this.planInCache++;
            }
        }
        if (tableList.isEmpty() && profile.getFullyTableList() != null) {
            tableList.addAll(profile.getFullyTableList());
        }
        this.statementType = profile.getStatementType();

        ExecProfile execProfile = profile.getExecProfile();
        if (execProfile != null) {
            this.sumJobLatency += execProfile.duration;
            if (this.maxJobLatency < execProfile.duration) {
                this.maxJobLatency = execProfile.duration;
            }
            this.avgJobLatency = sumJobLatency / execCount;
            if (plan == null) {
                plan = execProfile.dumpTree(new byte[0]);
            }
            if (binaryPlan == null) {
                binaryPlan = execProfile.binaryPlanOp();
            }
            // foreach profile to calculate avgKvLatency and set
            // foreach profile to calculate operator duration

            if ("select".equals(statementType)) {
                this.sumResultCount += execProfile.count;
                if (this.maxResultCount < execProfile.count) {
                    this.maxResultCount = execProfile.count;
                }
                if (this.minResultCount > execProfile.count) {
                    this.minResultCount = execProfile.count;
                }
                this.avgResultCount = sumResultCount / execCount;
            }
            if ("delete".equals(statementType)
                  || "update".equals(statementType)
                  || "insert".equals(statementType)
                  || "is_dml".equals(statementType)) {
                if (execProfile.getLastTuple() != null && execProfile.getLastTuple().length > 0
                    && execProfile.getLastTuple()[0] instanceof Long) {
                    long affectRows = (long) execProfile.getLastTuple()[0];
                    this.sumAffectedRows += affectRows;
                    if (profile.isAutoCommit()) {
                        this.analyzeInc += affectRows;
                        autoAnalyze();
                    }
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

    public void autoAnalyze() {
        if (analyzeInc > 10000 && tableList != null && tableList.size() == 1 && tableList.get(0) != null) {
            String[] fullTables = tableList.get(0).split("\\.");
            String schemaName = fullTables[0];
            String tableName = fullTables[1];
            StmtSummaryMap.addAnalyzeEvent(schemaName, tableName, analyzeInc);
            analyzeInc = 0;
        }
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
                avgLockLatency, sumJobLatency, maxJobLatency, avgJobLatency, sumCommitLatency,
                maxCommitLatency, avgCommitLatency, sumPreWriteLatency, maxPreWriteLatency, avgPreWriteLatency,
                sumCleanLatency, maxCleanLatency, avgCleanLatency, sumResultCount, maxResultCount,
                avgResultCount, sumAffectedRows, maxAffectedRows,
                avgAffectedRows, plan, binaryPlan, planInCache,
                prepared, id, state, msg};
        } finally {
            lock.readLock().unlock();
        }
    }

    public static String getTime(long milliSeconds) {
        DateFormat dtf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return dtf.format(new Timestamp(milliSeconds));
    }
}
