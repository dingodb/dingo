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

package io.dingodb.web.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;

@Entity
@Data
@Builder
@IdClass(StmtId.class)
@NoArgsConstructor
@AllArgsConstructor
@Table(name="statements_summary")
public class Stmt {
    @Id
    private String schema;
    @Id
    private String sqlText;
    @Id
    private String instance;
    private String type;
    private String statementType;
    private String simpleUser;
    private int execCount;
    private long sumLatency;
    private long maxLatency;
    private long minLatency;
    private long avgLatency;
    private String firstSeen;
    private String lastSeen;
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
    private long sumExecLatency;
    private long maxExecLatency;
    private long avgExecLatency;
    private long sumCommitLatency;
    private long maxCommitLatency;
    private long avgCommitLatency;
    private long sumPrewriteLatency;
    private long maxPrewriteLatency;
    private long avgPrewriteLatency;
    private long sumCleanLatency;
    private long maxCleanLatency;
    private long avgCleanLatency;
    private long sumResultCount;
    private long maxResultCount;
    private long avgResultCount;
    private long sumAffectedRows;
    private long maxAffectedRows;
    private long avgAffectedRows;
    private String plan;
    private String binaryPlan;
    private long planInCache;
    private boolean prepared;
    private String id;
}
