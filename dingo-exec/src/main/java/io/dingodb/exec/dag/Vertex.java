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

package io.dingodb.exec.dag;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.common.CommonId;
import io.dingodb.common.Location;
import io.dingodb.common.type.DingoType;
import io.dingodb.exec.base.OutputHint;
import io.dingodb.exec.base.Task;
import io.dingodb.exec.operator.params.AbstractParams;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.LinkedList;
import java.util.List;

import static io.dingodb.common.util.Utils.sole;

@Getter
@JsonPropertyOrder({"id", "op", "params", "outList", "inList", "hint", "pin"})
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Vertex {

    @Setter
    @JsonProperty("id")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private CommonId id;
    @Setter
    private Task task;

    @JsonProperty("op")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private final CommonId op;
    @JsonProperty("params")
    @JsonSerialize(contentAs = AbstractParams.class)
    @JsonDeserialize(contentAs = AbstractParams.class)
    private final AbstractParams data;
    private final List<Edge> outList;
    private final List<Edge> inList;
    @Setter
    private OutputHint hint;
    @Setter
    private int pin;

    public Vertex(CommonId op, Object data) {
        this(op, data, new LinkedList<>(), new LinkedList<>());
    }

    public Vertex(CommonId op, Object data, List<Edge> outList, List<Edge> inList) {
        this.op = op;
        this.data = (AbstractParams) data;
        this.outList = outList;
        this.inList = inList;
    }

    @JsonCreator
    public Vertex(
        @JsonProperty("id") CommonId id,
        @JsonProperty("task") Task task,
        @JsonProperty("op") CommonId op,
        @JsonProperty("data") AbstractParams data,
        @JsonProperty("outList") List<Edge> outList,
        @JsonProperty("inList") List<Edge> inList,
        @JsonProperty("hint") OutputHint hint,
        @JsonProperty("pin") int pin
    ) {
        this.id = id;
        this.task = task;
        this.op = op;
        this.data = data;
        this.outList = outList;
        this.inList = inList;
        this.hint = hint;
        this.pin = pin;
    }

    public void init() {
        data.init(this);
    }

    public void addEdge(Edge edge) {
        outList.add(edge);
    }

    public void addEdges(List<Edge> edges) {
        this.outList.addAll(edges);
    }

    public void addIn(Edge edge) {
        inList.add(edge);
    }

    public void addIns(List<Edge> edges) {
        this.outList.addAll(edges);
    }

    public CommonId getTaskId() {
        return getTask().getId();
    }

    public void copyHint(@NonNull Vertex vertex) {
        setHint(vertex.getHint());
    }

    public Edge getSoleEdge() {
        return outList.isEmpty() ? new Edge(this, null) : sole(outList);
    }

    public DingoType getParasType() {
        return getTask().getParasType();
    }

    public void setParas(Object[] paras) {
        data.setParas(paras);
    }

    public void setStartTs(long startTs) {
        data.setStartTs(startTs);
    }

    public <T> T getParam() {
        return (T) data;
    }

    public void destroy() {
        getData().destroy();
    }

    public boolean isToSumUp() {
        return hint != null && hint.isToSumUp();
    }

    public Location getTargetLocation() {
        return hint != null ? hint.getLocation() : null;
    }

    public long getStartTs() {
        return getTask().getTxnId().seq;
    }
}
