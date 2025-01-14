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
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.dingodb.codec.CodecService;
import io.dingodb.codec.KeyValueCodec;
import io.dingodb.common.CommonId;
import io.dingodb.common.type.TupleMapping;
import io.dingodb.exec.dag.Vertex;
import io.dingodb.exec.expr.SqlExpr;
import io.dingodb.meta.entity.Column;
import io.dingodb.meta.entity.Table;
import lombok.Getter;

import java.util.Arrays;
import java.util.List;

@Getter
@JsonTypeName("documentScanFilter")
@JsonPropertyOrder({"indexTableId", "schema", "keyMapping", "isLookup", "selection", "queryString"})
public class TxnDocumentScanFilterParam extends FilterProjectSourceParam {

    @JsonProperty("indexTableId")
    @JsonSerialize(using = CommonId.JacksonSerializer.class)
    @JsonDeserialize(using = CommonId.JacksonDeserializer.class)
    private final CommonId indexTableId;
    @JsonProperty("isLookup")
    private final boolean isLookup;
    @JsonProperty("indexDefinition")
    protected final Table index;
    protected final Table table;
    private final KeyValueCodec codec;
    private transient KeyValueCodec lookupCodec;
    @JsonProperty("scanTs")
    private final long scanTs;
    private final long timeout;

    protected List<Integer> mapList;
    @JsonProperty("queryString")
    private final String queryString;

    public TxnDocumentScanFilterParam(
        CommonId partId,
        CommonId indexTableId,
        CommonId tableId,
        TupleMapping keyMapping,
        SqlExpr filter,
        TupleMapping selection,
        Table index,
        Table table,
        boolean isLookup,
        long scanTs,
        long timeout,
        String queryString
    ) {
        super(tableId, partId, table.tupleType(), table.version, filter, selection, keyMapping);
        this.indexTableId = indexTableId;
        this.isLookup = isLookup;
        this.index = index;
        this.table = table;
        this.scanTs = scanTs;
        this.timeout = timeout;
        this.codec = CodecService.getDefault().createKeyValueCodec(
            index.codecVersion,
            index.version,
            index.tupleType(),
            index.keyMapping()
        );
        this.queryString = queryString;
    }

    @Override
    public void init(Vertex vertex) {
        super.init(vertex);
        if (isLookup()) {
            lookupCodec = CodecService.getDefault().createKeyValueCodec(
                table.codecVersion,
                table.version,
                table.tupleType(),
                table.keyMapping()
            );
        } else {
            mapList = mapping(selection, table, index);
        }
    }

    private static List<Integer> mapping(TupleMapping selection, Table td, Table index) {
        Integer[] mappings = new Integer[selection.size()];
        for (int i = 0; i < selection.size(); i ++) {
            Column column = td.getColumns().get(selection.get(i));
            mappings[i] = index.getColumns().indexOf(column);
        }
        return Arrays.asList(mappings);
    }
}
