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

package io.dingodb.common.audit;

import io.dingodb.common.CommonId;
import io.dingodb.common.log.AuditLogUtils;
import io.dingodb.common.log.BackUpLogUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DingoAudit implements IAudit{
    private CommonId serverId;
    private String connId;
    private long jobSeqId;
    private String schema;
    private String user;
    private String client;
    private long startTs;
    private long forUpdateTs;
    private String transactionType;
    private String txIsolation;
    private String sqlType;
    private String sql;
    private boolean isAutoCommit;

    public DingoAudit(String txIsolation, String transactionType) {
        this.txIsolation = txIsolation;
        this.transactionType = transactionType;
        this.isAutoCommit = true;
    }
    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();
        return str.append("[").append("serverId=").append(serverId).append("] ")
            .append("[").append("conn=").append(connId).append("] ")
            .append("[").append("jobSeqId=").append(jobSeqId).append("] ")
            .append("[").append("user=").append(user).append("] ")
            .append("[").append("schema=").append(schema).append("] ")
            .append("[").append("client=").append(client).append("] ")
            .append("[").append("startTs=").append(startTs).append("] ")
            .append("[").append("forUpdateTs=").append(forUpdateTs).append("] ")
            .append("[").append("transactionType=").append(transactionType).append("] ")
            .append("[").append("txIsolation=").append(txIsolation).append("] ")
            .append("[").append("isAutoCommit=").append(isAutoCommit).append("] ")
            .append("[").append("sqlType=").append(sqlType).append("] ")
            .append("[").append("sql=").append(sql).append("]").toString();
    }

    @Override
    public void printAudit(boolean isDisableAudit) {
        AuditLogUtils.info(isDisableAudit, this.toString());
    }

    @Override
    public void printIncrementBackup(boolean isDisableIncrementBackup) {
        BackUpLogUtils.info(isDisableIncrementBackup, this.toString());
    }
}
