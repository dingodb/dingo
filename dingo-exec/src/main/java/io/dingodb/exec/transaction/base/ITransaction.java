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
import io.dingodb.exec.table.Part;
import io.dingodb.exec.transaction.impl.TransactionCache;
import io.dingodb.net.Channel;

import java.util.Map;

public interface ITransaction {

    long getStart_ts();

    long getCommit_ts();

    CommonId getTxnId();

    CommonId getTxnInstanceId();

    int getIsolationLevel();

    void setIsolationLevel(int isolationLevel);

    TransactionStatus getStatus();

    void setStatus(TransactionStatus status);

    Part getPart();

    void setPart(Part part);

    long getLockTtl();

    TransactionCache getCache();

    Map<CommonId, Channel> getChannelMap();

    void registerChannel(CommonId commonId, Channel channel);

    boolean commitPrimaryKey();

    byte[] getPrimaryKey();

    void commit(JobManager jobManager);

    void rollback(JobManager jobManager);

    void close();
    Job getJob();
}
