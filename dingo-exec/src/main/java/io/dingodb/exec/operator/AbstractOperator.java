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

package io.dingodb.exec.operator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.CommonId;
import io.dingodb.exec.base.Operator;
import io.dingodb.exec.base.Task;
import lombok.Getter;
import lombok.Setter;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    property = "type"
)
@JsonSubTypes({
    @JsonSubTypes.Type(AggregateOperator.class),
    @JsonSubTypes.Type(EmptySourceOperator.class),
    @JsonSubTypes.Type(CoalesceOperator.class),
    @JsonSubTypes.Type(FilterOperator.class),
    @JsonSubTypes.Type(GetByIndexOperator.class),
    @JsonSubTypes.Type(GetByKeysOperator.class),
    @JsonSubTypes.Type(HashJoinOperator.class),
    @JsonSubTypes.Type(HashOperator.class),
    @JsonSubTypes.Type(LikeScanOperator.class),
    @JsonSubTypes.Type(PartCountOperator.class),
    @JsonSubTypes.Type(PartDeleteOperator.class),
    @JsonSubTypes.Type(PartInsertOperator.class),
    @JsonSubTypes.Type(PartitionOperator.class),
    @JsonSubTypes.Type(PartRangeDeleteOperator.class),
    @JsonSubTypes.Type(PartRangeScanOperator.class),
    @JsonSubTypes.Type(PartUpdateOperator.class),
    @JsonSubTypes.Type(ProjectOperator.class),
    @JsonSubTypes.Type(ReceiveOperator.class),
    @JsonSubTypes.Type(ReduceOperator.class),
    @JsonSubTypes.Type(RootOperator.class),
    @JsonSubTypes.Type(SendOperator.class),
    @JsonSubTypes.Type(SortOperator.class),
    @JsonSubTypes.Type(SumUpOperator.class),
    @JsonSubTypes.Type(ValuesOperator.class),
    @JsonSubTypes.Type(RemovePartOperator.class),
})
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class AbstractOperator implements Operator {
    @Getter
    @Setter
    @JsonProperty("id")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    protected CommonId id;
    @Getter
    @Setter
    protected Task task;

    @Override
    public void setParas(Object[] paras) {
    }
}
