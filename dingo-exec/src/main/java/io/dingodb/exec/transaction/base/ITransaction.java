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

package io.dingodb.exec.transaction.base;

import io.dingodb.common.CommonId;
import io.dingodb.exec.base.Job;
import io.dingodb.exec.base.JobManager;
import io.dingodb.exec.transaction.impl.TransactionCache;
import io.dingodb.net.Channel;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

public interface ITransaction {

    long getStartTs();

    long getCommitTs();

    CommonId getTxnId();

    CommonId getTxnInstanceId();

    int getIsolationLevel();

    TransactionType getType();

    TransactionStatus getStatus();

    TransactionCache getCache();

    Map<CommonId, Channel> getChannelMap();

    void registerChannel(CommonId commonId, Channel channel);

    boolean commitPrimaryKey(CacheToObject cacheToObject);

    byte[] getPrimaryKey();

    void commit(JobManager jobManager);

    void rollback(JobManager jobManager);

    void rollBackPessimisticLock(JobManager jobManager);

    void rollBackPessimisticPrimaryLock(JobManager jobManager);

    void close(JobManager jobManager);

    void cancel();

    void addSql(String sql);

    List<String> getSqlList();

    boolean isAutoCommit();

    void setAutoCommit(boolean autoCommit);

    boolean isPessimistic();

    long getForUpdateTs();

    void setForUpdateTs(long forUpdateTs);

    byte[] getPrimaryKeyLock();

    void setPrimaryKeyLock(byte[] primaryKeyLock);

    void setPrimaryKeyFuture(Future future);

    Job getJob();

    void setJob(Job job);

    void setTransactionConfig(Properties sessionVariables);

    long getLockTimeOut();

    CompletableFuture<Void> getFinishedFuture();
}
