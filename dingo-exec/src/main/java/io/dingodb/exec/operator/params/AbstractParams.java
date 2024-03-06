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

package io.dingodb.exec.operator.params;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.CommonId;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.operator.data.Context;
import io.dingodb.exec.table.Part;
import io.dingodb.exec.transaction.params.CleanCacheParam;
import io.dingodb.exec.transaction.params.CommitParam;
import io.dingodb.exec.transaction.params.PessimisticResidualLockParam;
import io.dingodb.exec.transaction.params.PessimisticRollBackParam;
import io.dingodb.exec.transaction.params.PessimisticRollBackScanParam;
import io.dingodb.exec.transaction.params.PreWriteParam;
import io.dingodb.exec.transaction.params.RollBackParam;
import io.dingodb.exec.transaction.params.ScanCacheParam;
import io.dingodb.exec.transaction.params.ScanCacheResidualLockParam;
import io.dingodb.exec.transaction.params.ScanCleanCacheParam;
import lombok.Getter;
import lombok.Setter;

@Getter
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type"
)
@JsonSubTypes({
    @JsonSubTypes.Type(AggregateParams.class),
    @JsonSubTypes.Type(EmptySourceParam.class),
    @JsonSubTypes.Type(CoalesceParam.class),
    @JsonSubTypes.Type(FilterParam.class),
    @JsonSubTypes.Type(GetByIndexParam.class),
    @JsonSubTypes.Type(GetByKeysParam.class),
    @JsonSubTypes.Type(HashJoinParam.class),
    @JsonSubTypes.Type(HashParam.class),
    @JsonSubTypes.Type(LikeScanParam.class),
    @JsonSubTypes.Type(IndexMergeParam.class),
    @JsonSubTypes.Type(PartCountParam.class),
    @JsonSubTypes.Type(PartDeleteParam.class),
    @JsonSubTypes.Type(PartInsertParam.class),
    @JsonSubTypes.Type(PartitionParam.class),
    @JsonSubTypes.Type(PartRangeDeleteParam.class),
    @JsonSubTypes.Type(PartRangeScanParam.class),
    @JsonSubTypes.Type(PartUpdateParam.class),
    @JsonSubTypes.Type(ProjectParam.class),
    @JsonSubTypes.Type(ReceiveParam.class),
    @JsonSubTypes.Type(ReduceRelOpParam.class),
    @JsonSubTypes.Type(ReduceParam.class),
    @JsonSubTypes.Type(RootParam.class),
    @JsonSubTypes.Type(RelOpParam.class),
    @JsonSubTypes.Type(ScanParam.class),
    @JsonSubTypes.Type(ScanWithRelOpParam.class),
    @JsonSubTypes.Type(SendParam.class),
    @JsonSubTypes.Type(SortParam.class),
    @JsonSubTypes.Type(SumUpParam.class),
    @JsonSubTypes.Type(ValuesParam.class),
    @JsonSubTypes.Type(RemovePartParam.class),
    @JsonSubTypes.Type(PartVectorParam.class),
    @JsonSubTypes.Type(VectorPartitionParam.class),
    @JsonSubTypes.Type(VectorPointDistanceParam.class),
    @JsonSubTypes.Type(TxnPartInsertParam.class),
    @JsonSubTypes.Type(TxnPartUpdateParam.class),
    @JsonSubTypes.Type(TxnPartDeleteParam.class),
    @JsonSubTypes.Type(TxnLikeScanParam.class),
    @JsonSubTypes.Type(TxnPartRangeDeleteParam.class),
    @JsonSubTypes.Type(TxnPartRangeScanParam.class),
    @JsonSubTypes.Type(TxnScanParam.class),
    @JsonSubTypes.Type(TxnScanWithRelOpParam.class),
    @JsonSubTypes.Type(CommitParam.class),
    @JsonSubTypes.Type(PreWriteParam.class),
    @JsonSubTypes.Type(RollBackParam.class),
    @JsonSubTypes.Type(ScanCacheParam.class),
    @JsonSubTypes.Type(CompareAndSetParam.class),
    @JsonSubTypes.Type(PessimisticLockDeleteParam.class),
    @JsonSubTypes.Type(PessimisticLockInsertParam.class),
    @JsonSubTypes.Type(PessimisticLockUpdateParam.class),
    @JsonSubTypes.Type(PessimisticRollBackParam.class),
    @JsonSubTypes.Type(PessimisticRollBackScanParam.class),
    @JsonSubTypes.Type(DistributionSourceParam.class),
    @JsonSubTypes.Type(DistributionParam.class),
    @JsonSubTypes.Type(GetDistributionParam.class),
    @JsonSubTypes.Type(ScanCleanCacheParam.class),
    @JsonSubTypes.Type(CleanCacheParam.class),
    @JsonSubTypes.Type(CopyParam.class),
    @JsonSubTypes.Type(TxnGetByKeysParam.class),
    @JsonSubTypes.Type(TxnGetByIndexParam.class),
    @JsonSubTypes.Type(PessimisticLockParam.class),
    @JsonSubTypes.Type(PessimisticResidualLockParam.class),
    @JsonSubTypes.Type(ScanCacheResidualLockParam.class)
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class AbstractParams {

    @JsonProperty("part")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    @Setter
    protected CommonId partId;

    protected transient Part part = null;

    @Setter
    protected transient Context context;

    public AbstractParams() {
    }

    public AbstractParams(CommonId partId, Part part) {
        this.partId = partId;
        this.part = part;
    }

    public void init(Vertex vertex) {
        // todo move param initialize to init
    }

    public void setParas(Object[] paras) {
    }

    public void setStartTs(long startTs) {

    }

    public void destroy() {

    }
}
