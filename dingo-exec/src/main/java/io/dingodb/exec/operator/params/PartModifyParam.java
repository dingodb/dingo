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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.partition.RangeDistribution;
import io.dingodb.common.type.DingoType;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.meta.entity.Table;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

@Getter
public abstract class PartModifyParam extends AbstractParams {

    @JsonProperty("table")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    protected final CommonId tableId;
    @JsonProperty("schema")
    protected final DingoType schema;
    @JsonProperty("keyMapping")
    protected final TupleMapping keyMapping;
    @Setter
    protected long count;
    protected Table table;
    protected KeyValueCodec codec;
    @Setter
    protected NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions;
    protected List<Boolean> keyState;

    public PartModifyParam(
        CommonId tableId,
        CommonId partId,
        DingoType schema,
        TupleMapping keyMapping,
        Table table,
        NavigableMap<ByteArrayUtils.ComparableByteArray, RangeDistribution> distributions
    ) {
        super(partId, null);
        this.tableId = tableId;
        this.schema = schema;
        this.keyMapping = keyMapping;
        this.codec = CodecService.getDefault().createKeyValueCodec(table.tupleType(), table.keyMapping());
        this.table = table;
        this.distributions = distributions;
    }

    @Override
    public void init(Vertex vertex) {
        count = 0;
        keyState = new ArrayList<>();
    }

    public void addKeyState(boolean state) {
        keyState.add(state);
    }

    public Boolean[] getKeyState() {
        return keyState.toArray(new Boolean[0]);
    }

    public void reset() {
        count = 0;
        keyState.clear();
    }
}
